"""ORM row -> response schema conversion.

Explicit rather than `from_attributes`, matching the sibling annexes: the wire
exposes `device_id` where the column is `public_id`, `capacity_kva` is a
`Numeric` that must reach JSON as a float, and the internal integer primary key
must never leave the building. Keeping the translation in one place means a
column rename cannot silently change the public contract.
"""

import base64
import io

import segno

from api.live.schemas import DeviceOut, EnrollmentTokenOut
from api.live.service import IssuedToken
from api.live_public.schemas import (
    BrokerInfo,
    CredentialsInfo,
    EnrollResponse,
    TopicsInfo,
)
from api.live_public.service import EnrolmentResult
from core.config import settings
from shared.const import DeviceStatus, DeviceType
from shared.models.local_models import DeviceModel


def to_device_out(row: DeviceModel) -> DeviceOut:
    return DeviceOut(
        # The PUBLIC id. `row.id` is internal and is never exposed.
        device_id=row.public_id,
        name=row.name,
        type=DeviceType(row.type),
        status=DeviceStatus(row.status),
        ean=row.ean,
        pure_injection=row.pure_injection,
        # Numeric -> float at the boundary: psycopg hands back a Decimal, which
        # pydantic would serialise as a string and a chart would then plot as a
        # category.
        capacity_kva=float(row.capacity_kva) if row.capacity_kva is not None else None,
        connector_name=row.connector_name,
        connector_version=row.connector_version,
        enrolled_at=row.enrolled_at,
        created_at=row.created_at,
    )


def to_token_out(issued: IssuedToken) -> EnrollmentTokenOut:
    return EnrollmentTokenOut(
        token=issued.token,
        expires_at=issued.expires_at,
        qr_svg=_qr_data_uri(issued.token),
    )


def _qr_data_uri(token: str) -> str:
    """The token as an inline SVG data URI, rendered server-side.

    Inline rather than a second endpoint, because a URL carrying a credential
    lands in nginx's and KrakenD's access logs - and the token is the only thing
    standing between a stranger and a device on someone's roof.

    segno writes BYTES even for SVG, so this needs a BytesIO; handing it a
    StringIO raises `TypeError: string argument expected, got 'bytes'`.
    """
    buffer = io.BytesIO()
    segno.make(token, error="m").save(buffer, kind="svg", scale=4, border=2)
    encoded = base64.b64encode(buffer.getvalue()).decode("ascii")
    return f"data:image/svg+xml;base64,{encoded}"


def to_enroll_response(result: EnrolmentResult) -> EnrollResponse:
    """protocol 5.2. Everything the device needs, in one response.

    `broker.host` is the PUBLIC address, never `settings.MQTT_HOST`. A device
    stores this in NVS and never asks again, so handing out a compose service
    name bricks it permanently - there is no OTA and the fix is a site visit.
    `core/config.py` asserts the two differ in staging and production.
    """
    return EnrollResponse(
        device_id=str(result.device.public_id),
        broker=BrokerInfo(
            host=settings.BROKER_PUBLIC_HOST,
            port=settings.BROKER_PUBLIC_PORT,
            tls=settings.BROKER_PUBLIC_TLS,
        ),
        credentials=CredentialsInfo(
            username=str(result.device.public_id),
            # Shown ONCE and never recoverable: we do not store it, the broker
            # keeps only a hash, and a lost secret means re-enrolment.
            password=result.password,
        ),
        topics=TopicsInfo(
            telemetry=result.telemetry_topic,
            status=result.status_topic,
        ),
    )
