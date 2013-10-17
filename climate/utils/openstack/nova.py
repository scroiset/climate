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

from novaclient import exceptions as nova_exception
from novaclient.v1_1 import client as nova_client

from climate.openstack.common import log as logging
from climate.utils.openstack import base

LOG = logging.getLogger(__name__)


def client(ctx):
    compute_url = base.url_for(ctx.service_catalog, 'compute')

    nova = nova_client.Client(ctx.user_name,
                              ctx.auth_token, ctx.tenant_id,
                              auth_url=compute_url)

    nova.client.auth_token = ctx.auth_token
    nova.client.management_url = compute_url

    return nova


def get(instance_id, ctx):
    return client(ctx).servers.get(instance_id)


def wake_up(instance_id, ctx):
    try:
        client(ctx).servers.wake_up(instance_id)
    except AttributeError:
        LOG.exception('No wake_up method implemented in Nova client found.')


def delete(instance_id, ctx):
    try:
        client(ctx).servers.delete(instance_id)
    except nova_exception.NotFound:
        pass


def suspend(instance_id, ctx):
    client(ctx).servers.suspend(instance_id)


def create_image(instance_id, ctx):
    instance = get(instance_id, ctx)
    instance_name = instance.name
    client(ctx).servers.create_image(instance_id,
                                     "reserved_%s" % instance_name)


def backup(instance_id):
    raise NotImplementedError
