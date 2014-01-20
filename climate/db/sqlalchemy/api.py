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

"""Implementation of SQLAlchemy backend."""

import sys

import sqlalchemy as sa
from sqlalchemy.sql.expression import asc
from sqlalchemy.sql.expression import desc

from climate import context
from climate.db.sqlalchemy import models
from climate.openstack.common.db import exception as db_exc
from climate.openstack.common.db.sqlalchemy import session as db_session
from climate.openstack.common.gettextutils import _  # noqa
from climate.openstack.common import log as logging


LOG = logging.getLogger(__name__)

get_engine = db_session.get_engine
get_session = db_session.get_session


def get_backend():
    """The backend is this module itself."""
    return sys.modules[__name__]


def model_query(model, session=None, project_only=None):
    """Query helper.

    :param model: base model to query
    :param project_only: if present and current context is user-type,
            then restrict query to match the project_id from current context.
    """
    session = session or get_session()

    query = session.query(model)

    if project_only:
        ctx = context.current()
        query = query.filter_by(tenant_id=ctx.project_id)

    return query


def column_query(*columns, **kwargs):
    session = kwargs.get("session") or get_session()

    query = session.query(*columns)

    if kwargs.get("project_only"):
        ctx = context.current()
        query = query.filter_by(tenant_id=ctx.tenant_id)

    return query


def setup_db():
    try:
        engine = db_session.get_engine(sqlite_fk=True)
        models.Lease.metadata.create_all(engine)
    except sa.exc.OperationalError as e:
        LOG.error(_("Database registration exception: %s"), e)
        return False
    return True


def drop_db():
    try:
        engine = db_session.get_engine(sqlite_fk=True)
        models.Lease.metadata.drop_all(engine)
    except Exception as e:
        LOG.error(_("Database shutdown exception: %s"), e)
        return False
    return True


## Helpers for building constraints / equality checks


def constraint(**conditions):
    return Constraint(conditions)


def equal_any(*values):
    return EqualityCondition(values)


def not_equal(*values):
    return InequalityCondition(values)


class Constraint(object):
    def __init__(self, conditions):
        self.conditions = conditions

    def apply(self, model, query):
        for key, condition in self.conditions.iteritems():
            for clause in condition.clauses(getattr(model, key)):
                query = query.filter(clause)
        return query


class EqualityCondition(object):
    def __init__(self, values):
        self.values = values

    def clauses(self, field):
        return sa.or_([field == value for value in self.values])


class InequalityCondition(object):
    def __init__(self, values):
        self.values = values

    def clauses(self, field):
        return [field != value for value in self.values]


#Reservation
def _reservation_get(session, reservation_id):
    query = model_query(models.Reservation, session)
    return query.filter_by(id=reservation_id).first()


def reservation_get(reservation_id):
    return _reservation_get(get_session(), reservation_id)


def reservation_get_all():
    query = model_query(models.Reservation, get_session())
    return query.all()


def reservation_get_all_by_lease_id(lease_id):
    reservations = model_query(models.Reservation, get_session()).\
        filter_by(lease_id=lease_id)
    return reservations.all()


def reservation_get_all_by_values(**kwargs):
    """Returns all entries filtered by col=value."""

    reservation_query = model_query(models.Reservation, get_session())
    for name, value in kwargs.items():
        column = getattr(models.Reservation, name, None)
        if column:
            reservation_query = reservation_query.filter(column == value)
    return reservation_query.all()


def reservation_create(values):
    values = values.copy()
    reservation = models.Reservation()
    reservation.update(values)

    session = get_session()
    with session.begin():
        try:
            reservation.save(session=session)
        except db_exc.DBDuplicateEntry as e:
            # raise exception about duplicated columns (e.columns)
            raise RuntimeError("DBDuplicateEntry: %s" % e.columns)

    return reservation_get(reservation.id)


def reservation_update(reservation_id, values):
    session = get_session()

    with session.begin():
        reservation = _reservation_get(session, reservation_id)
        reservation.update(values)
        reservation.save(session=session)

    return reservation_get(reservation_id)


def reservation_destroy(reservation_id):
    session = get_session()
    with session.begin():
        reservation = _reservation_get(session, reservation_id)

        if not reservation:
            # raise not found error
            raise RuntimeError("Reservation not found!")

        session.delete(reservation)


#Lease
def _lease_get(session, lease_id):
    query = model_query(models.Lease, session)
    return query.filter_by(id=lease_id).first()


def lease_get(lease_id):
    return _lease_get(get_session(), lease_id)


def lease_get_all():
    query = model_query(models.Lease, get_session())
    return query.all()


def lease_get_all_by_tenant(tenant_id):
    raise NotImplementedError


def lease_get_all_by_user(user_id):
    raise NotImplementedError


def lease_list():
    return model_query(models.Lease, get_session()).all()


def lease_create(values):
    values = values.copy()
    lease = models.Lease()
    reservations = values.pop("reservations", [])
    events = values.pop("events", [])
    lease.update(values)

    session = get_session()
    with session.begin():
        try:
            lease.save(session=session)

            for r in reservations:
                reservation = models.Reservation()
                reservation.update({"lease_id": lease.id})
                reservation.update(r)
                reservation.save(session=session)

            for e in events:
                event = models.Event()
                event.update({"lease_id": lease.id})
                event.update(e)
                event.save(session=session)

        except db_exc.DBDuplicateEntry as e:
            # raise exception about duplicated columns (e.columns)
            raise RuntimeError("DBDuplicateEntry: %s" % e.columns)

    return lease_get(lease.id)


def lease_update(lease_id, values):
    session = get_session()

    with session.begin():
        lease = _lease_get(session, lease_id)
        lease.update(values)
        lease.save(session=session)

    return lease_get(lease_id)


def lease_destroy(lease_id):
    session = get_session()
    with session.begin():
        lease = _lease_get(session, lease_id)

        if not lease:
            # raise not found error
            raise RuntimeError("Lease not found!")

        session.delete(lease)


#Event
def _event_get(session, event_id):
    query = model_query(models.Event, session)
    return query.filter_by(id=event_id).first()


def _event_get_all(session):
    query = model_query(models.Event, session)
    return query


def event_get(event_id):
    return _event_get(get_session(), event_id)


def event_get_all():
    return _event_get_all(get_session()).all()


def event_get_all_sorted_by_filters(sort_key, sort_dir, filters):
    """Return events filtered and sorted by name of the field."""

    sort_fn = {'desc': desc, 'asc': asc}

    events_query = _event_get_all(get_session())

    if 'status' in filters:
        events_query = \
            events_query.filter(models.Event.status == filters['status'])
    if 'lease_id' in filters:
        events_query = \
            events_query.filter(models.Event.lease_id == filters['lease_id'])
    if 'event_type' in filters:
        events_query = events_query.filter(models.Event.event_type ==
                                           filters['event_type'])

    events_query = events_query.order_by(
        sort_fn[sort_dir](getattr(models.Event, sort_key))
    )

    return events_query.all()


def event_list():
    return model_query(models.Event.id, get_session()).all()


def event_create(values):
    values = values.copy()
    event = models.Event()
    event.update(values)

    session = get_session()
    with session.begin():
        try:
            event.save(session=session)
        except db_exc.DBDuplicateEntry as e:
            # raise exception about duplicated columns (e.columns)
            raise RuntimeError("DBDuplicateEntry: %s" % e.columns)

    return event_get(event.id)


def event_update(event_id, values):
    session = get_session()

    with session.begin():
        event = _event_get(session, event_id)
        event.update(values)
        event.save(session=session)

    return event_get(event_id)


def event_destroy(event_id):
    session = get_session()
    with session.begin():
        event = _event_get(session, event_id)

        if not event:
            # raise not found error
            raise RuntimeError("Event not found!")

        session.delete(event)


#ComputeHostReservation
def _host_reservation_get(session, host_reservation_id):
    query = model_query(models.ComputeHostReservation, session)
    return query.filter_by(id=host_reservation_id).first()


def host_reservation_get(host_reservation_id):
    return _host_reservation_get(get_session(),
                                 host_reservation_id)


def host_reservation_get_all():
    query = model_query(models.ComputeHostReservation, get_session())
    return query.all()


def _host_reservation_get_by_reservation_id(session, reservation_id):
    query = model_query(models.ComputeHostReservation, session)
    return query.filter_by(reservation_id=reservation_id).first()


def host_reservation_get_by_reservation_id(reservation_id):
    return _host_reservation_get_by_reservation_id(get_session(),
                                                   reservation_id)


def host_reservation_create(values):
    values = values.copy()
    host_reservation = models.ComputeHostReservation()
    host_reservation.update(values)

    session = get_session()
    with session.begin():
        try:
            host_reservation.save(session=session)
        except db_exc.DBDuplicateEntry as e:
            # raise exception about duplicated columns (e.columns)
            raise RuntimeError("DBDuplicateEntry: %s" % e.columns)

    return host_reservation_get(host_reservation.id)


def host_reservation_update(host_reservation_id, values):
    session = get_session()

    with session.begin():
        host_reservation = _host_reservation_get(session,
                                                 host_reservation_id)
        host_reservation.update(values)
        host_reservation.save(session=session)

    return host_reservation_get(host_reservation_id)


def host_reservation_destroy(host_reservation_id):
    session = get_session()
    with session.begin():
        host_reservation = _host_reservation_get(session,
                                                 host_reservation_id)

        if not host_reservation:
            # raise not found error
            raise RuntimeError("Host Reservation not found!")

        session.delete(host_reservation)


#ComputeHostAllocation
def _host_allocation_get(session, host_allocation_id):
    query = model_query(models.ComputeHostAllocation, session)
    return query.filter_by(id=host_allocation_id).first()


def host_allocation_get(host_allocation_id):
    return _host_allocation_get(get_session(),
                                host_allocation_id)


def host_allocation_get_all():
    query = model_query(models.ComputeHostAllocation, get_session())
    return query.all()


def host_allocation_get_all_by_values(**kwargs):
    """Returns all entries filtered by col=value."""
    allocation_query = model_query(models.ComputeHostAllocation, get_session())
    for name, value in kwargs.items():
        column = getattr(models.ComputeHostAllocation, name, None)
        if column:
            allocation_query = allocation_query.filter(column == value)
    return allocation_query.all()


def host_allocation_create(values):
    values = values.copy()
    host_allocation = models.ComputeHostAllocation()
    host_allocation.update(values)

    session = get_session()
    with session.begin():
        try:
            host_allocation.save(session=session)
        except db_exc.DBDuplicateEntry as e:
            # raise exception about duplicated columns (e.columns)
            raise RuntimeError("DBDuplicateEntry: %s" % e.columns)

    return host_allocation_get(host_allocation.id)


def host_allocation_update(host_allocation_id, values):
    session = get_session()

    with session.begin():
        host_allocation = _host_allocation_get(session,
                                               host_allocation_id)
        host_allocation.update(values)
        host_allocation.save(session=session)

    return host_allocation_get(host_allocation_id)


def host_allocation_destroy(host_allocation_id):
    session = get_session()
    with session.begin():
        host_allocation = _host_allocation_get(session,
                                               host_allocation_id)

        if not host_allocation:
            # raise not found error
            raise RuntimeError("Host Allocation not found!")

        session.delete(host_allocation)


#ComputeHost
def _host_get(session, host_id):
    query = model_query(models.ComputeHost, session)
    return query.filter_by(id=host_id).first()


def _host_get_all(session):
    query = model_query(models.ComputeHost, session)
    return query


def host_get(host_id):
    return _host_get(get_session(), host_id)


def host_list():
    return model_query(models.ComputeHost, get_session()).all()


def host_get_all_by_filters(filters):
    """Returns hosts filtered by name of the field."""

    hosts_query = _host_get_all(get_session())

    if 'status' in filters:
        hosts_query = hosts_query.\
            filter(models.ComputeHost.status == filters['status'])

    return hosts_query.all()


def host_get_all_by_queries(queries):
    """Returns hosts filtered by an array of queries.

    :param queries: array of queries "key op value" where op can be
        http://docs.sqlalchemy.org/en/rel_0_7/core/expression_api.html
            #sqlalchemy.sql.operators.ColumnOperators

    """
    hosts_query = model_query(models.ComputeHost, get_session())
    key_not_found = []

    oper = dict({'<': 'lt', '>': 'gt', '<=': 'le', '>=': 'ge', '==': 'eq',
                 '!=': 'ne'})
    for query in queries:
        try:
            key, op, value = query.split(' ', 3)
        except ValueError:
            raise RuntimeError('Invalid filter: %s' % query)
        column = getattr(models.ComputeHost, key, None)
        if column:
            if op == 'in':
                filt = column.in_(value.split(','))
            else:
                if op in oper:
                    op = oper[op]
                try:
                    attr = filter(lambda e: hasattr(column, e % op),
                                  ['%s', '%s_', '__%s__'])[0] % op
                except IndexError:
                    raise RuntimeError('Invalid filter operator: %s' % op)
                if value == 'null':
                    value = None
                filt = getattr(column, attr)(value)
            hosts_query = hosts_query.filter(filt)
        else:
            key_not_found.append(key)

    hosts = []
    for query in queries:
        try:
            key, op, value = query.split(' ', 3)
        except ValueError:
            raise RuntimeError('Invalid filter: %s' % query)

        extra_filter = model_query(
            models.ComputeHostExtraCapability, get_session()).\
            filter(models.ComputeHostExtraCapability.capability_name == key).\
            all()
        if not extra_filter and key in key_not_found:
            raise RuntimeError('Invalid filter column: %s' % key)
        for line in extra_filter:
            if line.capability_value >= value and op == '<':
                hosts.append(line.computehost_id)
            elif line.capability_value <= value and op == '>':
                hosts.append(line.computehost_id)
            elif line.capability_value > value and op == '<=':
                hosts.append(line.computehost_id)
            elif line.capability_value < value and op == '>=':
                hosts.append(line.computehost_id)
            elif line.capability_value != value and op == '==':
                hosts.append(line.computehost_id)
            elif line.capability_value == value and op == '!=':
                hosts.append(line.computehost_id)

    return hosts_query.filter(~models.ComputeHost.id.in_(hosts)).all()


def host_create(values):
    values = values.copy()
    host = models.ComputeHost()
    host.update(values)

    session = get_session()
    with session.begin():
        try:
            host.save(session=session)
        except db_exc.DBDuplicateEntry as e:
            # raise exception about duplicated columns (e.columns)
            raise RuntimeError("DBDuplicateEntry: %s" % e.columns)

    return host_get(host.id)


def host_update(host_id, values):
    session = get_session()

    with session.begin():
        host = _host_get(session, host_id)
        host.update(values)
        host.save(session=session)

    return host_get(host_id)


def host_destroy(host_id):
    session = get_session()
    with session.begin():
        host = _host_get(session, host_id)

        if not host:
            # raise not found error
            raise RuntimeError("Host not found!")

        session.delete(host)


#ComputeHostExtraCapability
def _host_extra_capability_get(session, host_extra_capability_id):
    query = model_query(models.ComputeHostExtraCapability, session)
    return query.filter_by(id=host_extra_capability_id).first()


def host_extra_capability_get(host_extra_capability_id):
    return _host_extra_capability_get(get_session(),
                                      host_extra_capability_id)


def _host_extra_capability_get_all_per_host(session, host_id):
    query = model_query(models.ComputeHostExtraCapability, session)
    return query.filter_by(computehost_id=host_id)


def host_extra_capability_get_all_per_host(host_id):
    return _host_extra_capability_get_all_per_host(get_session(),
                                                   host_id).all()


def host_extra_capability_create(values):
    values = values.copy()
    host_extra_capability = models.ComputeHostExtraCapability()
    host_extra_capability.update(values)

    session = get_session()
    with session.begin():
        try:
            host_extra_capability.save(session=session)
        except db_exc.DBDuplicateEntry as e:
            # raise exception about duplicated columns (e.columns)
            raise RuntimeError("DBDuplicateEntry: %s" % e.columns)

    return host_extra_capability_get(host_extra_capability.id)


def host_extra_capability_update(host_extra_capability_id, values):
    session = get_session()

    with session.begin():
        host_extra_capability = \
            _host_extra_capability_get(session,
                                       host_extra_capability_id)
        host_extra_capability.update(values)
        host_extra_capability.save(session=session)

    return host_extra_capability_get(host_extra_capability_id)


def host_extra_capability_destroy(host_extra_capability_id):
    session = get_session()
    with session.begin():
        host_extra_capability = \
            _host_extra_capability_get(session,
                                       host_extra_capability_id)

        if not host_extra_capability:
            # raise not found error
            raise RuntimeError("Host Extracapability not found!")

        session.delete(host_extra_capability)


def host_extra_capability_get_all_per_name(host_id, capability_name):
    session = get_session()

    with session.begin():
        query = _host_extra_capability_get_all_per_host(session, host_id)
        return query.filter_by(capability_name=capability_name).all()
