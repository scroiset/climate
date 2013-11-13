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

from oslo.config import cfg

from climate import config
from climate import context
from climate import tests

from climate.plugins.oshosts import reservation_pool as rp


class AggregateFake(object):

    def __init__(self, i, name, hosts):
        self.id = i
        self.name = name
        self.hosts = hosts


class ReservationPoolTestCase(tests.TestCase):

    def setUp(self):
        super(ReservationPoolTestCase, self).setUp()
        config.parse_configs()
        self.pool_name = 'pool-name-xxx'
        self.tenant_id = 'tenant-uuid'
        self.fake_aggregate = AggregateFake(i=123,
                                            name='fooname',
                                            hosts=['host1', 'host2']
                                            )
        self.freepool_name = cfg.CONF['physical:host'].aggregate_freepool_name
        self.fake_freepool = AggregateFake(i=456,
                                           name=self.freepool_name,
                                           hosts=['host3']
                                           )

        self.set_context(context.ClimateContext(tenant_id=self.tenant_id))

        self.nova = mock.MagicMock()
        mock.patch('novaclient.client.Client',
                   return_value=self.nova).start()

        self.nova.aggregates.set_metadata = mock.MagicMock()
        self.nova.aggregates.remove_host = mock.MagicMock()

        self.pool = rp.ReservationPool()

        self.p_name = self.patch(self.pool, '_generate_aggregate_name')
        self.p_name.return_value = self.pool_name

    def _patch_get_agg_from_whatever(self):
        def get_fake_aggregate(*args):
            if self.freepool_name in args:
                return self.fake_freepool
            else:
                return self.fake_aggregate

        self.patch(self.pool, 'get_aggregate_from_name_or_id')\
            .side_effect = get_fake_aggregate

    def test_get_agg_from_id(self):
        self.pool.get_aggregate_from_id(123)
        self.nova.aggregates.get\
                 .assert_called_once_with(123)

        self.nova.aggregates.get.reset_mock()
        self.pool.get_aggregate_from_id("456")
        self.nova.aggregates.get\
                 .assert_called_once_with(456)

        self.nova.aggregates.get.reset_mock()
        self.pool.get_aggregate_from_id(self.fake_aggregate)
        self.nova.aggregates.get\
                 .assert_called_once_with(self.fake_aggregate.id)

    def test_get_agg_from_name(self):
        self.nova.aggregates.list.return_value = [self.fake_aggregate]

        self.assertEqual(self.pool.get_aggregate_from_name('none'),
                         None)
        self.assertEqual(self.pool.get_aggregate_from_name('fooname'),
                         self.fake_aggregate)

    def test_get_agg_from_whatever(self):
        self.nova.aggregates.list.return_value = [self.fake_aggregate]

        self.assertEqual(self.pool.get_aggregate_from_name_or_id('none'), None)
        self.assertEqual(self.pool.get_aggregate_from_name_or_id('fooname'),
                         self.fake_aggregate)

        self.nova.aggregates.get.reset_mock()
        self.pool.get_aggregate_from_name_or_id(123)
        self.nova.aggregates.get\
                 .assert_called_once_with(123)

        self.nova.aggregates.get.reset_mock()
        self.pool.get_aggregate_from_name_or_id("456")
        self.nova.aggregates.get\
                 .assert_called_once_with(456)

        self.nova.aggregates.get.reset_mock()
        self.pool.get_aggregate_from_name_or_id(self.fake_aggregate)
        self.nova.aggregates.get\
                 .assert_called_once_with(self.fake_aggregate.id)

    def test_create(self):
        self.nova.aggregates.create = mock.MagicMock(
            return_value=self.fake_aggregate
        )

        agg = self.pool.create()

        self.assertEqual(agg, self.fake_aggregate)

        az_name = rp.CLIMATE_AZ_PREFIX + self.pool_name
        self.nova.aggregates.create\
                            .assert_called_once_with(self.pool_name,
                                                     az_name)

        meta = {rp.CLIMATE_OWNER: self.tenant_id}
        self.nova.aggregates.set_metadata\
                            .assert_called_once_with(self.fake_aggregate, meta)

    def test_create_no_az(self):
        self.nova.aggregates.create = mock.MagicMock(
            return_value=self.fake_aggregate
        )

        self.pool.create(az=False)

        self.nova.aggregates.create.assert_called_once_with(self.pool_name)

    def test_delete_with_host(self):
        self._patch_get_agg_from_whatever()
        agg = self.pool.get('foo')

        self.pool.delete(agg)
        self.nova.aggregates.delete.assert_called_once_with(agg.id)
        for h in agg.hosts:
            self.nova.aggregates.remove_host.assert_any_call(agg.id, h)
            self.nova.aggregates.add_host.assert_any_call(
                self.fake_freepool.name,
                h
            )

        # can't delete aggregate with hosts
        self.assertRaises(rp.AggregateHaveHost,
                          self.pool.delete, 'bar',
                          force=False)

    def test_delete_with_no_host(self):
        self._patch_get_agg_from_whatever()
        agg = self.pool.get('foo')
        agg.hosts = []
        self.pool.delete('foo', force=False)
        self.nova.aggregates.delete.assert_called_once_with(agg.id)

    def test_get_all(self):
        self.pool.get_all()
        self.nova.aggregates.list.assert_called_once_with()

    def test_get(self):
        self._patch_get_agg_from_whatever()
        agg = self.pool.get('foo')
        self.assertEqual(self.fake_aggregate,
                         agg)

    def test_add_computehost(self):
        self._patch_get_agg_from_whatever()
        self.pool.add_computehost('pool', 'host3')

        self.nova.aggregates.add_host\
                            .assert_any_call(self.fake_aggregate.id,
                                             'host3')
        self.nova.aggregates.remove_host\
                            .assert_any_call(self.fake_aggregate.id,
                                             'host3')

    def test_add_computehost_not_in_freepool(self):
        self._patch_get_agg_from_whatever()
        self.assertRaises(rp.HostNotInFreePool,
                          self.pool.add_computehost,
                          'foopool',
                          'ghost-host')

    def test_add_computehost_to_freepool(self):
        self._patch_get_agg_from_whatever()
        self.pool.add_computehost_to_freepool('host2')
        self.nova.aggregates.add_host\
                            .assert_called_once_with(self.fake_freepool.id,
                                                     'host2')

    def test_remove_computehost_from_freepool(self):
        self._patch_get_agg_from_whatever()
        self.pool.remove_computehost_from_freepool('host3')

        self.nova.aggregates.remove_host\
                            .assert_called_once_with(self.fake_freepool.id,
                                                     'host3')

    def test_remove_computehost_from_freepool_not_in(self):
        self._patch_get_agg_from_whatever()
        self.assertRaises(rp.HostNotInFreePool,
                          self.pool.remove_computehost_from_freepool,
                          'hostXX')

    def test_remove_allcomputehosts(self):
        self._patch_get_agg_from_whatever()
        self.pool.remove_all_computehosts('pool')
        for h in self.fake_aggregate.hosts:
            self.nova.aggregates.remove_host\
                                .assert_any_call(self.fake_aggregate.id,
                                                 h)

    def test_get_computehosts(self):
        self._patch_get_agg_from_whatever()
        hosts = self.pool.get_computehosts('foo')
        self.assertEqual(hosts, self.fake_aggregate.hosts)

    def test_add_project(self):
        self._patch_get_agg_from_whatever()
        self.pool.add_project('pool', 'projectX')
        self.nova.aggregates.set_metadata\
                 .assert_called_once_with(self.fake_aggregate.id,
                                          {'projectX': rp.TENANT_ID_KEY}
                                          )

    def test_remove_project(self):
        self._patch_get_agg_from_whatever()
        self.pool.remove_project('pool', 'projectY')
        self.nova.aggregates.set_metadata\
                 .assert_called_once_with(self.fake_aggregate.id,
                                          {'projectY': None}
                                          )
