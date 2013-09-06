# Copyright (c) 2013 Mirantis Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import datetime
import six

from oslo.config import cfg
from stevedore import enabled

from climate import context
from climate.db import api as db_api
from climate import exceptions
from climate.openstack.common import log as logging
from climate.openstack.common.rpc import service as rpc_service

manager_opts = [
    cfg.StrOpt('rpc_topic',
               default='climate.manager',
               help='The topic Climate uses for climate-manager messages.'),
    cfg.ListOpt('plugins',
                default=['dummy.vm.plugin'],
                help='All plugins to use (one for every resource type to '
                     'support.)'),
]

CONF = cfg.CONF
CONF.register_opts(manager_opts, 'manager')
LOG = logging.getLogger(__name__)


class ManagerService(rpc_service.Service):
    """Service class for the climate-manager service.

    Responsible for working with Climate DB, scheduling logic, running events,
    working with plugins, etc.
    """

    RPC_API_VERSION = '1.0'

    def __init__(self, host):
        super(ManagerService, self).__init__(host, CONF.manager.rpc_topic)
        self.plugins = self._get_plugins()
        self.resource_actions = self._setup_actions()

    def start(self):
        super(ManagerService, self).start()
        self.tg.add_timer(10, self._event)

    def _get_plugins(self):
        """Return dict of resource-plugin class pairs."""
        config_plugins = CONF.plugins
        plugins = {}

        extension_manager = enabled.EnabledExtensionManager(
            check_func=lambda ext: ext.name in config_plugins,
            namespace='climate.resource.plugins',
            invoke_on_load=True
        )

        for ext in extension_manager.extensions:
            if ext.obj.resource_type in plugins:
                raise exceptions.ClimateException(
                    'You have provided several plugins for one resource type '
                    'in configuration file. '
                    'Please set one plugin per resource type.'
                )

            plugins[ext.obj.resource_type] = ext.obj

        if len(plugins) < len(config_plugins):
            raise exceptions.ClimateException('Not all requested plugins are '
                                              'loaded.')

        return plugins

    def _setup_actions(self):
        """Setup actions for each resource type supported.

        BasePlugin interface provides only on_start and on_end behaviour now.
        If there are some configs needed by plugin, they should be returned
        from get_plugin_opts method. These flags are registered in
        [resource_type] group of configuration file.
        """
        actions = {}

        for resource_type, plugin in six.iteritems(self.plugins):
            plugin = self.plugins[resource_type]
            CONF.register_opts(plugin.get_plugin_opts(), group=resource_type)

            actions[resource_type] = {}
            actions[resource_type]['on_start'] = plugin.on_start
            actions[resource_type]['on_end'] = plugin.on_end

        return actions

    def _event(self):
        """Tries to commit event.

        If there is an event in Climate DB to be done, do it and change its
        status to 'DONE'.
        """
        with context.Context():
            LOG.debug('Trying to get event from DB.')
            events = db_api.event_get_all_sorted_by_filters(
                sort_key='time',
                sort_dir='asc',
                filters={'status': 'UNDONE'}
            )

            if not events:
                return

            event = events[0]

            if event['time'] < datetime.datetime.now():
                event_type = event['event_type']
                event_fn = getattr(self, event_type, None)
                if event_fn is None:
                    raise exceptions.ClimateException('Event type %s is not '
                                                      'supported' % event_type)
                event_fn(self, event['lease_id'])

            db_api.event_update(event['id'], {'status': 'DONE'})

    def get_lease(self, lease_id):
        return db_api.lease_get(lease_id)

    def list_leases(self):
        return db_api.lease_list()

    def create_lease(self, lease_values):
        start_date = lease_values['start_date']
        end_date = lease_values['end_date']

        if start_date == 'now':
            start_date = datetime.datetime.now()
        else:
            start_date = datetime.datetime.strptime(start_date,
                                                    "%Y-%m-%d %H:%M")
        end_date = datetime.datetime.strptime(end_date, "%Y-%m-%d %H:%M")

        lease_values['start_date'] = start_date
        lease_values['end_date'] = end_date
        if not lease_values.get('events'):
            lease_values['events'] = []

        lease_values['events'].append({'event_type': 'start_lease',
                                       'time': start_date})
        lease_values['events'].append({'event_type': 'end_lease',
                                       'time': end_date})

        lease = db_api.lease_create(lease_values)

        return db_api.lease_get(lease['id'])

    def update_lease(self, lease_id, values):
        if values:
            db_api.lease_update(lease_id, values)
        return db_api.lease_get(lease_id)

    def delete_lease(self, lease_id):
        lease = self.get_lease(lease_id)
        for reservation in lease['reservations']:
            self.plugins[reservation['resource_type']]\
                .delete(reservation['resource_id'])
        db_api.lease_destroy(lease_id)

    def start_lease(self, lease_id):
        self._basic_action(lease_id, 'on_start', 'active')

    def end_lease(self, lease_id):
        self._basic_action(lease_id, 'on_end', 'deleted')

    def _basic_action(self, lease_id, action_time, reservation_status=None):
        """Commits basic lease actions such as starting and ending."""
        lease = self.get_lease(lease_id)

        for reservation in lease['reservations']:
            resource_type = reservation['resource_type']
            self.resource_actions[resource_type][action_time](
                reservation['resource_id']
            )

            if reservation_status is not None:
                db_api.reservation_update(reservation['id'],
                                          {'status': reservation_status})
