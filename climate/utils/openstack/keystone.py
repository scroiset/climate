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

import json

from keystoneclient.v3 import client as keystone_client_v3
from oslo.config import cfg

from climate import context
from climate.utils.openstack import base


opts = [
    cfg.StrOpt('identity_service',
               default='identityv3',
               help='Identity service to use.')
]

CONF = cfg.CONF
CONF.register_cli_opts(opts)


def client(username=None, password=None, trust_id=None, auth_url=None,
           tenant=None, ctx=None):
    """Return Keystone client for defined in 'identity_service' conf."""
    ctx = ctx or context.Context.current()
    tenant_name = tenant or ctx.tenant_name
    auth_url = auth_url or base.url_for(ctx.service_catalog,
                                        CONF.identity_service)
    if not username:
        username = ctx.user_name

    keystone = keystone_client_v3.Client(username=username,
                                         token=ctx.auth_token,
                                         password=password,
                                         tenant_name=tenant_name,
                                         auth_url=auth_url,
                                         trust_id=trust_id)
    keystone.management_url = auth_url
    return keystone


def create_ctx_from_trust(trust_id):
    """Return context built from given trust."""
    ctx = context.Context()
    ctx.user_name = CONF.os_admin_username
    ctx.tenant_name = CONF.os_admin_tenant_name
    auth_url = "%s://%s:%s/v3" % (CONF.os_auth_protocol,
                                  CONF.os_auth_host,
                                  CONF.os_auth_port)
    keystone_client = client(
        password=CONF.os_admin_password,
        trust_id=trust_id,
        auth_url=auth_url,
        ctx=ctx
    )

    ctx.auth_token = keystone_client.auth_token
    ctx.service_catalog = json.dumps(
        keystone_client.service_catalog.catalog['catalog']
    )

    # use 'with ctx' statement in the place you need context from trust
    return ctx
