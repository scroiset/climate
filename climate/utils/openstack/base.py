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


def url_for(service_catalog, service_type, admin=False,
            endpoint_interface=None):
    """Gets url of the service to communicate through."""
    if not endpoint_interface:
        endpoint_interface = 'public'
    if admin:
        endpoint_interface = 'admin'

    service = _get_service_from_catalog(service_catalog, service_type)

    if service:
        endpoints = service['endpoints']
        try:
            # if Keystone API v3 endpoints returned
            endpoint = [e for e in endpoints
                        if e['interface'] == endpoint_interface][0]
            return endpoint['url']
        except KeyError:
            # otherwise
            return endpoints[0]['%sURL' % endpoint_interface]
    else:
        raise Exception('Service "%s" not found' % service_type)


def _get_service_from_catalog(catalog, service_type):
    """Parses JSON catalog string and returns info about needed service."""
    if catalog:
        catalog = json.loads(catalog)
        for service in catalog:
            if service['type'] == service_type:
                return service

    return None
