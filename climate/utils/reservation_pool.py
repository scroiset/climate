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

from climate import context
from climate.openstack.common import log as logging
from oslo.config import cfg

from novaclient import client
import novaclient.exceptions as nova_exceptions

import uuid as uuidgen

LOG = logging.getLogger(__name__)
CONF = cfg.CONF

TENANT_ID_KEY = 'climate:tenant'
CLIMATE_OWNER = 'climate:owner'
CLIMATE_AZ_PREFIX = 'climate:'


class AggregateWrapper(object):
    def __init__(self):
        self.ctx = context.Context.current()
        #TODO(scroiset): use catalog to find the url
        auth_url = "%s://%s:%s/v2.0" % (CONF.os_auth_protocol,
                                        CONF.os_auth_host,
                                        CONF.os_auth_port
                                        )
        self.nova = client.Client('2',
                                  username=CONF.os_admin_username,
                                  api_key=CONF.os_admin_password,
                                  auth_url=auth_url,
                                  project_id=CONF.os_admin_tenant_name
                                  )

    def _get_aggregate_from_name(self, name):

        #FIXME(scrosiet): can't get an aggregate by name
        # so iter over all aggregate and check for the good one!
        all_aggregates = self.nova.aggregates.list()
        for agg in all_aggregates:
            if name == agg.name:
                return agg

    def _get_aggregate_from_id(self, pool):
        """pool can be an aggregate or an id."""
        try:
            i = int(pool)
            # pool is an id
            return self.nova.aggregates.get(i)
        except Exception:
            if hasattr(pool, 'id'):
                # pool is an aggregate
                return self.nova.aggregates.get(pool.id)

    def get_aggregate_from_whatever(self, whatever):
        a = self._get_aggregate_from_id(whatever)
        if a is not None:
            return a
        return self._get_aggregate_from_name(whatever)

    def _get_aggregate_name(self):
        return str(uuidgen.uuid4())

    def create(self, name=None, az=True):
        """Create a Pool (an Aggregate)."""

        name = self._get_aggregate_name()

        if az:
            az = "%s%s" % (CLIMATE_AZ_PREFIX, name)
        else:
            az = None
        LOG.debug('Creating pool aggregate: %s with AZ %s' % (name, az))
        try:
            a = self.nova.aggregates.create(name, az)
            setmeta = self.nova.aggregates.set_metadata
            setmeta(a, {CLIMATE_OWNER: self.ctx.tenant_id})
            return a
        except Exception, e:
            raise e

    def delete(self, pool, force=True):
        """Delete an aggregate.

        pool can be an aggregate name or an aggregate id.
        Remove all hosts before delete aggregate (default).
        If force is False, raise exception if at least one
        host is attached to.

        """

        agg = self.get_aggregate_from_whatever(pool)

        if agg is None:
            LOG.warning("No aggregate associate with name or id %s" % pool)
            return

        hosts = agg.hosts
        if len(hosts) > 0 and not force:
            raise Exception("Can't delete Aggregate '%s', "
                            "host(s) attached to it ; %s" % (pool,
                                                             ", ".join(hosts)))
        for h in hosts:
            LOG.debug("Removing host '%s' from aggregate "
                      "'%s')" % (h, agg.id))
            self.nova.aggregates.remove_host(agg.id, h)

        try:
            self.nova.aggregates.delete(agg.id)
        except Exception, e:
            LOG.error(e)
            raise e

    def get_all(self):
        """Return all aggregate."""

        return self.nova.aggregates.list()

    def get(self, pool):
        """return details for aggregate pool or None."""

        agg = self.get_aggregate_from_whatever(pool)
        if agg is None:
            return None
        return agg

    def get_computehosts(self, pool):
        """Return a list of compute host names."""

        agg = self.get_aggregate_from_whatever(pool)

        if agg:
            return agg.hosts
        return []

    def add_computehost(self, pool, host):
        """Add a compute host to an aggregate
        The `host` must exist otherwise raise an error
        Return the related aggregate.
        Raise an aggregate exception if something wrong.
        """

        agg = self.get_aggregate_from_whatever(pool)

        if agg is None or agg.id is None:
            LOG.warning("Can't add a computehost "
                        "to an inexistant Aggregate '%s' " % agg)
            return

        LOG.info("add host '%s' to aggregate %s" % (host, agg.id))
        return self.nova.aggregates.add_host(agg.id, host)

    def remove_all_computehost(self, pool):
        """Remove all compute hosts attached to an aggregate."""

        hosts = self.get_computehosts(pool)
        self.remove_computehost(pool, hosts)

    def remove_computehost(self, pool, hosts=[]):
        """Remove compute host(s) from an aggregate."""

        if not isinstance(hosts, list):
            hosts = [hosts]

        if hasattr(pool, 'id'):
            agg = pool
        else:
            agg = self.get_aggregate_from_whatever(pool)

        if agg is None:
            raise nova_exceptions.NotFound("Aggregate '%s' not found!" % pool)

        for h in hosts:
            try:
                self.nova.aggregates.remove_host(agg.id, h)
            except nova_exceptions.NotFound:
                pass

    def add_project(self, pool, project_id):
        """Add a project to an aggregate."""

        metadata = {project_id: TENANT_ID_KEY}

        agg = self.get_aggregate_from_whatever(pool)
        if agg is None:
            raise nova_exceptions.NotFound("No aggregate '%s'" % pool)

        return self.nova.aggregates.set_metadata(agg.id, metadata)

    def remove_project(self, pool, project_id):
        """Remove a project from an aggregate."""

        agg = self.get_aggregate_from_whatever(pool)
        if agg is None:
            raise nova_exceptions.NotFound("Can't add project to an"
                                           "nonexistent aggregate %s" % pool)
        metadata = {project_id: None}
        return self.nova.aggregates.set_metadata(agg.id, metadata)


ReservationPool = AggregateWrapper
