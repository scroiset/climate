# -*- encoding: utf-8 -*-
#
# Copyright © 2013 OpenStack Foundation
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

import time
import warnings

from climate import context
from climate import exceptions as exc
from climate.openstack.common import log as logging
from climate.utils.openstack import base
from climate.utils.openstack import nova
from oslo.config import cfg

from novaclient import client
import novaclient.exceptions as nova_exceptions

LOG = logging.getLogger(__name__)
CONF = cfg.CONF

PCLOUDS = False
try:
    from novaclient.v1_1.contrib import pclouds
    PCLOUDS = pclouds
except Exception:
    warnings.warn("Pcloud not supported by novaclient, "
                  "use workaround with aggregate")


class PcloudWrapper(object):
    """A wrapper on top of Pclouds
    and host aggregates.
    """

    def __init__(self):
        self.ctx = context.Context.current()
        self.nova = nova.client(self.ctx)
        keystone_url = base.url_for(self.ctx.service_catalog,
                                    'identity')
        self.nova_admin = client.Client('2',
                                        username=CONF.os_admin_username,
                                        api_key=CONF.os_admin_password,
                                        auth_url=keystone_url,
                                        project_id=CONF.os_admin_tenant_name
                                        )

    def _get_pcloud_id(self, pcloud):
        try:
            pid = pcloud.id
        except Exception:
            pid = pcloud

        return pid

    def _get_aggregate_id_from_pcloud_name(self, pcloud):
        pid = self._get_pcloud_id(pcloud)
        aggregate_name = "pcloud:%s" % pid

        #FIXME(scrosiet): can't get an aggregate by name
        # so iter over all aggregate and check for the good one!
        all_aggregates = self.nova_admin.aggregates.list()
        for agg in all_aggregates:
            if aggregate_name == agg.name:
                return agg

    def _get_pcloud_name(self):
        return "%s-%s-%s" % ('climate',
                             self.ctx.user_id,
                             time.time())

    def create(self, name=None):
        """Create a pcloud."""

        if name is None:
            name = self._get_pcloud_name()

        LOG.debug('Pcloud creation : %s' % name)
        try:
            return self.nova.pclouds.create(name)
        except Exception, e:
            raise e

    def delete(self, pcloud, force=True):
        """Delete a pcloud.

        pcloud can be a Pclouds or an uuid.
        Release all hosts before delete pcloud (default).
        If force is False, raise exception if one host is attached to.
        """

        agg = self._get_aggregate_id_from_pcloud_name(pcloud)

        if agg is None:
            LOG.warning("No aggregate associate with pcloud %s" % pcloud)
            return

        hosts = agg.hosts
        if len(hosts) > 0 and not force:
            raise Exception("Can't delete Pcloud %s, host attached to it")

        p = self.nova.pclouds.get_details(pcloud)
        for h in hosts:
            LOG.debug("remove host '%s' from pcloud "
                      "'%s' (aggregate '%s')" % (h, p.id, agg.id))
            self.nova.aggregates.remove_host(agg, h)

        try:
            self.nova.pclouds.delete(pcloud)
        except Exception, e:
            LOG.error(e)
            raise e

    def get_all(self):
        """Return all pclouds known."""
        return self.nova.pclouds.list()

    def get(self, pcloud):
        """return a Pclouds or None."""

        uuid = self._get_pcloud_id(pcloud)
        try:
            return self.nova.pclouds.get_details(uuid)
        except nova_exceptions.NotFound:
            raise exc.ClimateException('Pcloud "%s" not found' % uuid)

    def get_computehosts(self, pcloud):
        """Return a list of compute host names."""

        agg = self._get_aggregate_id_from_pcloud_name(pcloud)

        if agg:
            return agg.hosts
        return []

    def add_computehost(self, pcloud, host):
        """Add a compute host to a pcloud.
        The `host` must exist otherwise raise an error
        Return the related aggregate ...
        Raise an aggregate exception if something wrong.
        """

        pid = self._get_pcloud_id(pcloud)

        agg = self._get_aggregate_id_from_pcloud_name(pcloud)
        id_agg = agg.id

        if id_agg is None:
            LOG.warning("Can't add a computehost "
                        "to an inexistant Pcloud '%s' " % pid)
            return

        LOG.info("add host '%s' to aggregate "
                 "%s (pcloud %s)" % (host, id_agg, pid))
        return self.nova_admin.aggregates.add_host(id_agg, host)

    def remove_computehost(self, pcloud, hosts=[]):
        "Remove compute host(s) from a Pcloud."

        if not isinstance(hosts, list):
            hosts = [hosts]

        id_agg = self._get_aggregate_id_from_pcloud_name(pcloud)

        for h in hosts:
            try:
                self.nova_admin.aggregates.remove_host(id_agg, h)
            except nova_exceptions.NotFound:
                pass

    def add_project(self, pcloud, project_id):
        "Add a project to a pcloud."

        return self.nova.pclouds.add_project(pcloud, project_id)

    def remove_project(self, pcloud, project_id):
        return self.nova.pclouds.remove_project(pcloud, project_id)

    def set_cpu_allocation_ratio(self, pcloud, ratio):
        return self.nova.pclouds.set_cpu_allocation_ratio(pcloud, ratio)

    def set_ram_allocation_ratio(self, pcloud, ratio):
        return self.nova.pclouds.set_ram_allocation_ratio(pcloud, ratio)


if PCLOUDS:
    Pcloud = PcloudWrapper
