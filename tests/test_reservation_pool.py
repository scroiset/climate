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
        self.fake_aggregate = AggregateFake()
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
                                      return_value=self.fake_aggregate)
        self.p_aggregate.start()

        self.nova = mock.MagicMock()
        mock.patch('novaclient.client.Client',
                   return_value=self.nova).start()

        self.nova.aggregates.set_metadata = mock.MagicMock()
        self.nova.aggregates.remove_host = mock.MagicMock()

        self.pool = rp.AggregateWrapper()

    def test_create(self):
        self.nova.aggregates.create = mock.MagicMock(return_value=self.nova)

        agg = self.pool.create()

        self.assertEquals(agg, self.nova)

        az_name = rp.CLIMATE_AZ_PREFIX + self.pool_name
        self.nova.aggregates.create\
                            .assert_called_once_with(self.pool_name,
                                                     az_name)

        meta = {rp.CLIMATE_OWNER: self.tenant_id}
        self.nova.aggregates.set_metadata\
                            .assert_called_once_with(self.nova, meta)

    def test_create_no_az(self):
        self.nova.aggregates.create = mock.MagicMock(return_value=self.nova)

        self.pool.create(az=False)

        self.nova.aggregates.create.assert_called_once_with(self.pool_name,
                                                            None)

    def test_delete(self):
        self.nova.aggregates.delete = mock.MagicMock()

        agg = self.pool.get('foo')

        self.pool.delete('foo')
        self.nova.aggregates.delete.assert_called_once_with(agg.id)
        for h in agg.hosts:
            self.nova.aggregates.remove_host.assert_any_call(agg.id, h)

        # can't delete aggregate with hosts
        self.assertRaises(Exception, self.pool.create, 'bar', force=False)

        agg.hosts = []
        self.nova.aggregates.delete.reset_mock()
        self.pool.delete('foo', force=False)
        self.nova.aggregates.delete.assert_called_once_with(agg.id)

    def test_get_all(self):
        self.nova.aggregates.list = mock.MagicMock()
        self.pool.get_all()
        self.nova.aggregates.list.assert_called_once_with()

    def test_get(self):
        agg = self.pool.get('foo')
        self.assertEquals(self.fake_aggregate,
                          agg)

    def test_add_computehost(self):
        self.nova.aggregates.add_host = mock.MagicMock()

        self.pool.add_computehost('pool', 'host3')

        self.nova.aggregates.add_host\
                            .assert_called_once_with(AggregateFake().id,
                                                     'host3')

    def test_remove_allcomputehosts(self):
        self.pool.remove_all_computehost('pool')
        for h in AggregateFake().hosts:
            self.nova.aggregates.remove_host\
                                .assert_any_call(AggregateFake().id,
                                                 h)

    def test_get_computehosts(self):
        hosts = self.pool.get_computehosts('foo')
        self.assertEquals(hosts, AggregateFake().hosts)

    def test_add_project(self):
        self.pool.add_project('pool', 'projectX')
        self.nova.aggregates.set_metadata\
                 .assert_called_once_with(self.fake_aggregate.id,
                                          {'projectX': rp.TENANT_ID_KEY}
                                          )

    def test_remove_project(self):
        self.pool.remove_project('pool', 'projectY')
        self.nova.aggregates.set_metadata\
                 .assert_called_once_with(self.fake_aggregate.id,
                                          {'projectY': None}
                                          )
