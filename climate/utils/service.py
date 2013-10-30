#!/usr/bin/env python
# -*- encoding: utf-8 -*-
#
# Copyright © 2013  Julien Danjou <julien@danjou.info>
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

from oslo.config import cfg

from climate import context
from climate.openstack.common import log
from climate.openstack.common import rpc
import climate.openstack.common.rpc.proxy as rpc_proxy


class RpcProxy(rpc_proxy.RpcProxy):
    def cast(self, name, topic=None, version=None, ctx=None, **kwargs):
        if ctx is None:
            ctx = context.Context.current()
        msg = self.make_msg(name, **kwargs)
        return super(RpcProxy, self).cast(ctx, msg,
                                          topic=topic, version=version)

    def call(self, name, topic=None, version=None, ctx=None, **kwargs):
        if ctx is None:
            ctx = context.Context.current()
        msg = self.make_msg(name, **kwargs)
        return super(RpcProxy, self).call(ctx, msg,
                                          topic=topic, version=version)


def prepare_service(argv=[]):
    rpc.set_defaults(control_exchange='climate')
    cfg.set_defaults(log.log_opts,
                     default_log_levels=['amqplib=WARN',
                                         'qpid.messaging=INFO',
                                         'stevedore=INFO',
                                         'eventlet.wsgi.server=WARN'
                                         ])
    cfg.CONF(argv[1:], project='climate')
    log.setup('climate')
