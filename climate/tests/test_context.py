# Copyright (c) 2013 OpenStack Fondation
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

from climate import context
from climate import tests


class ContextTestCase(tests.TestCase):

    def setUp(self):
        super(ContextTestCase, self).setUp()

        self.ctx = {'tenant_id': 'tenantID',
                    'user_id': 'userID'
                    }
        self.ctxadm = {'tenant_id': 'tenantIDAdmin',
                       'user_id': 'userIDAdmin',
                       'is_admin': True
                       }

    def test_is_not_admin(self):
        c = context.Context(**self.ctx)
        self.assertEqual(c.is_admin, False)

    def test_is_admin(self):
        c = context.Context(**self.ctxadm)
        self.assertEqual(c.is_admin, True)

    def test_elevation(self):
        c = context.Context(**self.ctx)
        elevated = c.elevated()
        self.assertEqual(elevated.is_admin, True)

    def test_nested(self):
        with context.Context(**self.ctx) as c:
            self.assertEqual(c.tenant_id, 'tenantID')
            self.assertEqual(c.is_admin, False)
            with context.Context(**self.ctxadm) as a:
                self.assertEqual(c.tenant_id, 'tenantID')
                self.assertEqual(a.tenant_id, 'tenantIDAdmin')
                self.assertEqual(a.is_admin, True)
                self.assertEqual(c.is_admin, False)
