# Copyright (c) 2013 Openstack Fondation
# All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

import mock

from climate import config
from climate import context
from climate import test
from climate.utils import reservation_pool as rp

import novaclient.client


class AggregateFake(object):
    def __init__(self):
        self.id = 123
        self.name = 'fooname'
        self.hosts = ['host1', 'host2']


class ReservationPoolTestCase(test.TestCase):

    def setUp(self):
        super(ReservationPoolTestCase, self).setUp()
        self.pool_name = 'pool-name-xxx'
        self.tenant_id = 'tenant-uuid'
        self.set_context(context.Context(tenant_id=self.tenant_id)
                         )

        self.p_name = mock.patch('climate.utils.reservation_pool.'
                                 'AggregateWrapper.'
                                 '_get_aggregate_name',
                                 return_value=self.pool_name)
        self.p_name.start()
        self.p_aggregate = mock.patch('climate.utils.reservation_pool.'
                                      'AggregateWrapper.'
                                      'get_aggregate_from_whatever',
                                      return_value=AggregateFake())
        self.p_aggregate.start()

        self.nova = mock.MagicMock()
        mock.patch('novaclient.client.Client',
                   return_value=self.nova).start()

        self.pool = rp.AggregateWrapper()

    def test_create(self):
        self.nova.aggregates.create = mock.MagicMock(return_value=self.nova)
        self.nova.aggregates.set_metadata = mock.MagicMock()

        created = self.pool.create()

        self.nova.aggregates.create\
                            .assert_called_once_with(self.pool_name,
                                                     self.pool_name)
        self.nova.aggregates.set_metadata\
                            .assert_called_once_with(self.nova,
                                                     {rp.CLIMATE_OWNER:
                                                      self.tenant_id})

    def test_create_no_az(self):
        self.nova.aggregates.create = mock.MagicMock(return_value=self.nova)
        self.nova.aggregates.set_metadata = mock.MagicMock()

        created = self.pool.create(az=False)

        self.nova.aggregates.create.assert_called_once_with(self.pool_name,
                                                            None)

    def test_delete(self):
        self.nova.aggregates.delete = mock.MagicMock()
        self.nova.aggregates.remove_host = mock.MagicMock()

        agg = self.pool.get('foo')

        self.pool.delete('foo')
        self.nova.aggregates.delete.assert_called_once_with(agg.id)
        for h in agg.hosts:
            self.nova.aggregates.remove_host.assert_any_call(agg.id, h)

        # can't delete aggregate with hosts
        self.assertRaises(Exception, self.pool.create, 'bar', force=False)

        agg.hosts=[]
        self.nova.aggregates.delete.reset_mock()
        self.pool.delete('foo', force=False)
        self.nova.aggregates.delete.assert_called_once_with(agg.id)

    def test_add_computehost(self):
        self.nova.aggregates.add_host = mock.MagicMock()

        self.pool.add_computehost('pool', 'host3')

        self.nova.aggregates.add_host\
                            .assert_called_once_with(AggregateFake().id,
                                                     'host3')

    def test_add_project(self):
        pass

    def test_get_computehosts(self):
        hosts = self.pool.get_computehosts('foo')
        self.assertEquals(hosts, AggregateFake().hosts)
