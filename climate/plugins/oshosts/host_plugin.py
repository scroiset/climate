# -*- coding: utf-8 -*-
#
# Author: François Rossigneux <francois.rossigneux@inria.fr>
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

import json
import six

from novaclient import client
from oslo.config import cfg

from climate import context
from climate.db import api as db_api
from climate.db import utils as db_utils
from climate import exceptions
from climate.openstack.common.gettextutils import _  # noqa
from climate.openstack.common import uuidutils
from climate.plugins import base
from climate.plugins.oshosts import nova_inventory
from climate.plugins.oshosts import reservation_pool as rp
from climate.utils import service as service_utils


class CantAddExtraCapability(exceptions.ClimateException):
    msg_fmt = _("Can't add extracapabilities %(keys)s to Host %(host)s")


class HypervisorNotFound(exceptions.ClimateException):
    msg_fmt = _("Aggregate '%(pool)s' not found!")


class PhysicalHostPlugin(base.BasePlugin):
    """Plugin for physical host resource."""
    resource_type = 'physical:host'
    title = 'Physical Host Plugin'
    description = 'This plugin starts and shutdowns the hosts.'
    freepool_name = cfg.CONF[resource_type].aggregate_freepool_name
    pool = None
    inventory = None

    def __init__(self):
        #TODO(sbauza): use catalog to find the url
        auth_url = "%s://%s:%s/v2.0" % (cfg.CONF.os_auth_protocol,
                                        cfg.CONF.os_auth_host,
                                        cfg.CONF.os_auth_port)
        #TODO(scroiset): use client wrapped by climate and use trust
        self.nova = client.Client('2',
                                  username=cfg.CONF.climate_username,
                                  api_key=cfg.CONF.climate_password,
                                  auth_url=auth_url,
                                  project_id=cfg.CONF.climate_tenant_name)

    def create_reservation(self, values):
        """Create reservation."""
        pool = rp.ReservationPool()
        pool_name = uuidutils.generate_uuid()
        pool_instance = pool.create(name=pool_name)
        reservation_values = {
            'id': pool_name,
            'lease_id': values['lease_id'],
            'resource_id': pool_instance.id,
            'resource_type': values['resource_type'],
            'status': 'pending',
        }
        reservation = db_api.reservation_create(reservation_values)
        count_range = str(values['min']) + '-' + str(values['max'])
        host_values = {
            'reservation_id': reservation['id'],
            'resource_properties': values['resource_properties'],
            'hypervisor_properties': values['hypervisor_properties'],
            'count_range': count_range,
            'status': 'pending',
        }
        db_api.host_reservation_create(host_values)
        host_ids = self._matching_hosts(
            values['hypervisor_properties'],
            values['resource_properties'],
            count_range,
            values['start_date'],
            values['end_date'],
        )
        if not host_ids:
            raise RuntimeError('Not enough hosts available')
        for host_id in host_ids:
            db_api.host_allocation_create({'compute_host_id': host_id,
                                          'reservation_id': reservation['id']})

    def on_start(self, resource_id):
        """Add the hosts in the pool."""
        reservations = db_api.reservation_get_all_by_values(
            resource_id=resource_id)
        for reservation in reservations:
            pool = rp.ReservationPool()
            for allocation in db_api.host_allocation_get_all_by_values(
                    reservation_id=reservation['id']):
                host = db_api.host_get(allocation['compute_host_id'])
                host_name = host['hypervisor_hostname']
                pool.add_computehost(reservation['resource_id'], host_name)

    def on_end(self, resource_id):
        """Remove the hosts from the pool."""
        reservations = db_api.reservation_get_all_by_values(
            resource_id=resource_id)
        for reservation in reservations:
            db_api.reservation_update(reservation['id'],
                                      {'status': 'completed'})
            host_reservation = db_api.host_reservation_get_by_reservation_id(
                reservation['id'])
            db_api.host_reservation_update(host_reservation['id'],
                                           {'status': 'completed'})
            allocations = db_api.host_allocation_get_all_by_values(
                reservation_id=reservation['id'])
            pool = rp.ReservationPool()
            for allocation in allocations:
                db_api.host_allocation_destroy(allocation['id'])
                if self.nova.hypervisors.get(
                        self._get_hypervisor_from_name(
                        allocation['compute_host_id'])
                ).__dict__['running_vms'] == 0:
                    pool.delete(reservation['resource_id'])
                #TODO(frossigneux) Kill, migrate, or increase fees...

    def setup(self, conf):
        # Create freepool if not exists
        with context.ClimateContext() as ctx:
            ctx = ctx.elevated()
            if self.pool is None:
                self.pool = rp.ReservationPool()
            if self.inventory is None:
                self.inventory = nova_inventory.NovaInventory()
        if not self._freepool_exists():
            self.pool.create(name=self.freepool_name, az=None)

    def _freepool_exists(self):
        try:
            self.pool.get_aggregate_from_name_or_id(self.freepool_name)
            return True
        except rp.AggregateNotFound:
            return False

    def _get_extra_capabilities(self, host_id):
        extra_capabilities = {}
        raw_extra_capabilities = \
            db_api.host_extra_capability_get_all_per_host(host_id)
        for capability in raw_extra_capabilities:
            key = capability['capability_name']
            extra_capabilities[key] = capability['capability_value']
        return extra_capabilities

    @service_utils.export_context
    def get_computehost(self, host_id):
        host = db_api.host_get(host_id)
        extra_capabilities = self._get_extra_capabilities(host_id)
        if host is not None and extra_capabilities:
            res = host.copy()
            res.update(extra_capabilities)
            return res
        else:
            return host

    @service_utils.export_context
    def list_computehosts(self):
        raw_host_list = db_api.host_list()
        host_list = []
        for host in raw_host_list:
            host_list.append(self.get_computehost(host['id']))
        return host_list

    @service_utils.export_context
    def create_computehost(self, host_values):
        # TODO(sbauza):
        #  - Exception handling for HostNotFound
        host_id = host_values.pop('id', None)
        host_name = host_values.pop('name', None)

        host_ref = host_id or host_name
        if host_ref is None:
            raise nova_inventory.InvalidHost(host=host_values)
        servers = self.inventory.get_servers_per_host(host_ref)
        if servers:
            raise nova_inventory.HostHavingServers(host=host_ref,
                                                   servers=servers)
        host_details = self.inventory.get_host_details(host_ref)
        # NOTE(sbauza): Only last duplicate name for same extra capability will
        #  be stored
        to_store = set(host_values.keys()) - set(host_details.keys())
        extra_capabilities_keys = to_store
        extra_capabilities = dict(
            (key, host_values[key]) for key in extra_capabilities_keys
        )
        self.pool.add_computehost(self.freepool_name, host_ref)

        host = None
        cantaddextracapability = []
        try:
            host = db_api.host_create(host_details)
        except RuntimeError:
            #We need to rollback
            # TODO(sbauza): Investigate use of Taskflow for atomic transactions
            self.pool.remove_computehost(self.freepool_name, host_ref)
        if host:
            for key in extra_capabilities:
                values = {'computehost_id': host['id'],
                          'capability_name': key,
                          'capability_value': extra_capabilities[key],
                          }
                try:
                    db_api.host_extra_capability_create(values)
                except RuntimeError:
                    cantaddextracapability.append(key)
        if cantaddextracapability:
            raise CantAddExtraCapability(keys=cantaddextracapability,
                                         host=host['id'])
        if host:
            return self.get_computehost(host['id'])
        else:
            return None

    @service_utils.export_context
    def update_computehost(self, host_id, values):
        # NOTE (sbauza): Only update existing extra capabilites, don't create
        #  other ones
        if values:
            cantupdateextracapability = []
            for value in values:
                capabilities = db_api.host_extra_capability_get_all_per_name(
                    host_id,
                    value,
                )
                for raw_capability in capabilities:
                    capability = {
                        'capability_name': value,
                        'capability_value': values[value],
                    }
                    try:
                        db_api.host_extra_capability_update(
                            raw_capability['id'], capability)
                    except RuntimeError:
                        cantupdateextracapability.append(
                            raw_capability['capability_name'])
            if cantupdateextracapability:
                raise CantAddExtraCapability(host=host_id,
                                             keys=cantupdateextracapability)
        return self.get_computehost(host_id)

    @service_utils.export_context
    def delete_computehost(self, host_id):
        # TODO(sbauza):
        #  - Check if no leases having this host scheduled
        servers = self.inventory.get_servers_per_host(host_id)
        if servers:
            raise nova_inventory.HostHavingServers(host=host_id,
                                                   servers=servers)
        host = db_api.host_get(host_id)
        if not host:
            raise rp.HostNotFound(host=host_id)
        try:
            self.pool.remove_computehost(self.freepool_name,
                                         host['hypervisor_hostname'])
            # NOTE(sbauza): Extracapabilities will be destroyed thanks to
            #  the DB FK.
            db_api.host_destroy(host_id)
        except RuntimeError:
            # Nothing so bad, but we need to advert the admin he has to rerun
            raise rp.CantRemoveHost(host=host_id, pool=self.freepool_name)

    def _matching_hosts(self, hypervisor_properties, resource_properties,
                        count_range, start_date, end_date):
        """Return the matching hosts (preferably not allocated)

        """
        count_range = count_range.split('-')
        min_host = count_range[0]
        max_host = count_range[1]
        allocated_host_ids = []
        not_allocated_host_ids = []
        filter_array = []
        # TODO(frossigneux) support "or" operator
        if hypervisor_properties:
            filter_array = self._convert_requirements(
                hypervisor_properties)
        if resource_properties:
            filter_array += self._convert_requirements(
                resource_properties)
        for host in db_api.host_get_all_by_queries(filter_array):
            if not db_api.host_allocation_get_all_by_values(
                    compute_host_id=host['id']):
                not_allocated_host_ids.append(host['id'])
            elif db_utils.get_free_periods(
                host['id'],
                start_date,
                end_date,
                end_date - start_date,
            ) == [
                (start_date, end_date),
            ]:
                allocated_host_ids.append(host['id'])
        if len(not_allocated_host_ids) >= int(min_host):
            return not_allocated_host_ids[:int(max_host)]
        all_host_ids = allocated_host_ids + not_allocated_host_ids
        if len(all_host_ids) >= int(min_host):
            return all_host_ids[:int(max_host)]
        else:
            return []

    def _convert_requirements(self, requirements):
        """Convert the requirements to an array of strings.
        ["key op value", "key op value", ...]

        """
        # TODO(frossigneux) Support the "or" operator
        # Convert text to json
        if isinstance(requirements, six.string_types):
            requirements = json.loads(requirements)
        # Requirement list looks like ['<', '$ram', '1024']
        if self._requirements_with_three_elements(requirements):
                result = []
                if requirements[0] == '=':
                    requirements[0] = '=='
                string = (requirements[1][1:] + " " + requirements[0] + " " +
                          requirements[2])
                result.append(string)
                return result
        # Remove the 'and' element at the head of the requirement list
        elif self._requirements_with_and_keyword(requirements):
                return [self._convert_requirements(x)[0]
                        for x in requirements[1:]]
        # Empty requirement list
        elif isinstance(requirements, list) and not requirements:
            return requirements
        else:
            raise RuntimeError('Malformed requirements')

    def _requirements_with_three_elements(self, requirements):
        """Return true if requirement list looks like ['<', '$ram', '1024']."""
        return (isinstance(requirements, list) and
                len(requirements) == 3 and
                isinstance(requirements[0], six.string_types) and
                isinstance(requirements[1], six.string_types) and
                isinstance(requirements[2], six.string_types) and
                requirements[0] in ['==', '=', '!=', '>=', '<=', '>', '<'] and
                len(requirements[1]) > 1 and requirements[1][0] == '$' and
                len(requirements[2]) > 1)

    def _requirements_with_and_keyword(self, requirements):
        return (len(requirements) > 1 and
                isinstance(requirements[0], six.string_types) and
                requirements[0] == 'and' and
                all(self._convert_requirements(x) for x in requirements[1:]))

    def _get_hypervisor_from_name(self, hypervisor_name):
        """Return an hypervisor by name or an id."""
        hypervisor = None
        all_hypervisors = self.nova.hypervisors.list()
        for hyp in all_hypervisors:
            if hypervisor_name == hyp.hypervisor_hostname:
                hypervisor = hyp
        if hypervisor:
            return hypervisor
        else:
            raise HypervisorNotFound(pool=hypervisor_name)
