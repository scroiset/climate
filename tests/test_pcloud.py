#!/usr/bin/env python
# -*- encoding: utf-8 -*-
#
# Copyright © 2013 Julien Danjou <julien@danjou.info>
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

import mock
import unittest

from climate import context
#from climate.openstack.common import context
from climate import test
from climate.utils.openstack import nova as nova_cli
from climate.utils.openstack import base
from climate.utils.openstack import pcloud
from oslo.config import cfg


class PcloudTestCase(test.TestCase):

    _pcloud_name = 'pcloud-foo'

    def _nova_client(self, *args, **kwargs):
        return True

    def _pclouds(self, *args, **kwargs):
        return True

    @staticmethod
    def _pclouds_create(self, *args, **kwargs):
        m = mock.MagicMock()
        m.id = '0000'
        m.name = PcloudTestCase._pcloud_name
        return m

    def _list_empty_aggregates(self):
        return []

    @unittest.skipIf(pcloud.PCLOUDS is False, "No Pclouds no setup")
    def setUp(self):
        super(PcloudTestCase, self).setUp()
        self.set_context(context.get_admin_context())
        self.patch(base, 'url_for').return_value = 'http://localhost:9999'

        self.pcloud = pcloud.PcloudWrapper()

        self.pcloud._get_pcloud_name = mock.MagicMock(
            return_value=self._pcloud_name)

#        self.pcloud_name = self.patch(self.pcloud, '_get_pcloud_name').\
#                return_value(self._pcloud_name)
#
#        print "***"
#        print self.pcloud._get_pcloud_name()
#        print self.pcloud_name
#        print "***"
        cfg.CONF.set_override('os_admin_username', 'fooUser')
        self.addCleanup(cfg.CONF.reset)

    @unittest.skipIf(pcloud.PCLOUDS is False, "No Pclouds")
    def test_create_pcloud(self):

        m = self.patch(self.pcloud.nova.pclouds, 'create')
        m.side_effect = self._pclouds_create

        p = self.pcloud.create()

        print m.call_args_list
        m.assert_called_with(self._pcloud_name)
        self.assertEqual(p, self._pclouds_create())
