# Copyright (c) 2013 OpenStack Foundation
#
# Author: Swann Croiset <swann.croiset@bull.net>
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

import uuid as uuidgen

from novaclient import client
from novaclient import exceptions as nova_exceptions
from oslo.config import cfg

from climate import context
from climate.openstack.common import log as logging


LOG = logging.getLogger(__name__)

opts = [
    cfg.StrOpt('aggregate_freepool_name',
               default='freepool',
               help='Name of the special aggregate where all hosts '
                    'are candidate for physical host reservation'),
]

cfg.CONF.register_opts(opts, 'physical:host')

TENANT_ID_KEY = 'climate:tenant'
CLIMATE_OWNER = 'climate:owner'
CLIMATE_AZ_PREFIX = 'climate:'


class NoFreePool(Exception):
    pass


class HostNotInFreePool(Exception):
    msg = "Host %s not in freepool '%s'"

    def __init__(self, host, freepool_name):
        self.host = host
        self.freepool = freepool_name

    def __str__(self):
        return self.msg % (self.host,
                           self.freepool)


class CantRemoveHost(Exception):
    pass


class AggregateHaveHost(Exception):
    msg = "Can't delete Aggregate '%s', host(s) attached to it : %s"

    def __init__(self, pool):
        self.pool = pool

    def __str__(self):
        return self.msg % (self.pool.name,
                           ','.join(self.pool.hosts))


class AggregateNotFound(Exception):
    msg = "Aggregate '%s' not found!"

    def __init__(self, pool):
        self.pool = pool

    def __str__(self):
        return self.msg % self.pool


class ReservationPool(object):
    def __init__(self):
        self.ctx = context.current()
        self.freepool_name = cfg.CONF['physical:host'].aggregate_freepool_name

        #TODO(scroiset): use catalog to find the url
        auth_url = "%s://%s:%s/v2.0" % (cfg.CONF.os_auth_protocol,
                                        cfg.CONF.os_auth_host,
                                        cfg.CONF.os_auth_port
                                        )
        self.nova = client.Client('2',
                                  username=cfg.CONF.os_admin_username,
                                  api_key=cfg.CONF.os_admin_password,
                                  auth_url=auth_url,
                                  project_id=cfg.CONF.os_admin_tenant_name
                                  )

    def get_aggregate_from_name(self, name):

        #FIXME(scrosiet): can't get an aggregate by name
        # so iter over all aggregate and check for the good one!
        all_aggregates = self.nova.aggregates.list()
        for agg in all_aggregates:
            if name == agg.name:
                return agg
        else:
            return None

    def get_aggregate_from_id(self, aggregate_or_id):
        """id can be an aggregate or an id."""
        agg_id = None
        try:
            agg_id = int(aggregate_or_id)
        except (ValueError, TypeError):
            if hasattr(aggregate_or_id, 'id') and aggregate_or_id.id:
                # pool is an aggregate
                agg_id = aggregate_or_id.id

        if agg_id is not None:
            return self.nova.aggregates.get(agg_id)

    def get_aggregate_from_name_or_id(self, whatever):
        """Return an aggregate by name or an id."""

        aggregate = self.get_aggregate_from_id(whatever)
        if aggregate is None:
            return self.get_aggregate_from_name(whatever)
        return aggregate

    @staticmethod
    def _generate_aggregate_name():
        return str(uuidgen.uuid4())

    def create(self, az=True):
        """Create a Pool (an Aggregate) with or without Availability Zone.

        By default expose to user the aggregate with an Availability Zone.
        Return an aggregate or raise a nova exception.

        """

        name = self._generate_aggregate_name()

        az_name = None
        if az:
            az_name = "%s%s" % (CLIMATE_AZ_PREFIX, name)

        if az_name is None:
            LOG.debug('Creating pool aggregate: %s'
                      'without Availability Zone' % name)
            agg = self.nova.aggregates.create(name)
        else:
            LOG.debug('Creating pool aggregate: %s'
                      'with Availability Zone %s' % (name, az_name))
            agg = self.nova.aggregates.create(name, az_name)

        meta = {CLIMATE_OWNER: self.ctx.tenant_id}
        self.nova.aggregates.set_metadata(agg, meta)

        return agg

    def delete(self, pool, force=True):
        """Delete an aggregate.

        pool can be an aggregate name or an aggregate id.
        Remove all hosts before delete aggregate (default).
        If force is False, raise exception if at least one
        host is attached to.

        """

        agg = self.get_aggregate_from_name_or_id(pool)

        if agg is None:
            LOG.warning("No aggregate associate with name or id %s" % pool)
            return

        hosts = agg.hosts
        if len(hosts) > 0 and not force:
            raise AggregateHaveHost(agg)

        for host in hosts:
            LOG.debug("Removing host '%s' from aggregate "
                      "'%s')" % (host, agg.id))
            self.nova.aggregates.remove_host(agg.id, host)
            self.nova.aggregates.add_host(self.freepool_name, host)

        self.nova.aggregates.delete(agg.id)

    def get_all(self):
        """Return all aggregate."""

        return self.nova.aggregates.list()

    def get(self, pool):
        """return details for aggregate pool or None."""

        return self.get_aggregate_from_name_or_id(pool)

    def get_computehosts(self, pool):
        """Return a list of compute host names for an aggregate."""

        agg = self.get_aggregate_from_name_or_id(pool)

        if agg:
            return agg.hosts
        return []

    def add_computehost(self, pool, host):
        """Add a compute host to an aggregate.

        The `host` must exist otherwise raise an error
        and the `host` must be in the freepool.

        Return the related aggregate.
        Raise an aggregate exception if something wrong.
        """

        agg = self.get_aggregate_from_name_or_id(pool)

        if agg is None or agg.id is None:
            LOG.warning("Can't add a computehost "
                        "to an inexistant Aggregate '%s' " % agg)
            return

        freepool_agg = self.get(self.freepool_name)
        if freepool_agg is None:
            raise NoFreePool()

        if host not in freepool_agg.hosts:
            raise HostNotInFreePool(host, freepool_agg.name)

        LOG.info("removing host '%s' "
                 "to aggregate freepool %s" % (host, freepool_agg.name))
        self.remove_computehost(freepool_agg.id, host)

        LOG.info("adding host '%s' to aggregate %s" % (host, agg.id))
        try:
            return self.nova.aggregates.add_host(agg.id, host)
        except nova_exceptions.ClientException as e:
            LOG.exception(e)

    def add_computehost_to_freepool(self, host):
        freepool_agg = self.get(self.freepool_name)
        if freepool_agg is None:
            raise NoFreePool()

        if host in freepool_agg.hosts:
            LOG.warning('Host is already in the freepool')
            return

        return self.nova.aggregates.add_host(freepool_agg.id, host)

    def remove_all_computehosts(self, pool):
        """Remove all compute hosts attached to an aggregate."""

        hosts = self.get_computehosts(pool)
        self.remove_computehost(pool, hosts)

    def remove_computehost(self, pool, hosts):
        """Remove compute host(s) from an aggregate."""

        if not isinstance(hosts, list):
            hosts = [hosts]

        agg = self.get_aggregate_from_name_or_id(pool)
        if agg is None:
            raise AggregateNotFound(pool)

        freepool_agg = self.get(self.freepool_name)
        if freepool_agg is None:
            raise NoFreePool()

        for host in hosts:
            try:
                self.nova.aggregates.remove_host(agg.id, host)
                self.nova.aggregates.add_host(freepool_agg.id, host)
            except nova_exceptions.ClientException as e:
                raise CantRemoveHost(e)

    def remove_computehost_from_freepool(self, hosts):

        if not isinstance(hosts, list):
            hosts = [hosts]

        freepool_agg = self.get(self.freepool_name)
        if freepool_agg is None:
            raise NoFreePool()

        for host in hosts:
            if host not in freepool_agg.hosts:
                raise HostNotInFreePool(host, freepool_agg.name)

            try:
                self.nova.aggregates.remove_host(freepool_agg.id, host)
            except nova_exceptions.ClientException as e:
                raise CantRemoveHost(e)

    def add_project(self, pool, project_id):
        """Add a project to an aggregate."""

        metadata = {project_id: TENANT_ID_KEY}

        agg = self.get_aggregate_from_name_or_id(pool)
        if agg is None:
            raise nova_exceptions.NotFound("No aggregate '%s'" % pool)

        return self.nova.aggregates.set_metadata(agg.id, metadata)

    def remove_project(self, pool, project_id):
        """Remove a project from an aggregate."""

        agg = self.get_aggregate_from_name_or_id(pool)
        if agg is None:
            raise nova_exceptions.NotFound("Can't add project to an"
                                           "nonexistent aggregate %s" % pool)
        metadata = {project_id: None}
        return self.nova.aggregates.set_metadata(agg.id, metadata)
