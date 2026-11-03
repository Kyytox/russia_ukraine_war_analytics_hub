from fastapi import APIRouter

router = APIRouter(prefix="/api/raid-alerts", tags=["raid-alerts"])


page_content = [
    {
        "id": "raid-alerts-1",
        "image": "/img/raid_alerts_1.png",
        "url": "https://alerts.in.ua/en",
    },
    {
        "id": "raid-alerts-6",
        "image": "/img/raid_alerts_6.png",
        "url": "https://map.nahr.uk/en/",
    },
    {
        "id": "raid-alerts-4",
        "image": "/img/raid_alerts_4.png",
        "url": "https://neptun.in.ua/",
    },
    {
        "id": "raid-alerts-7",
        "image": "/img/raid_alerts_7.png",
        "url": "https://argusmap.online/",
    },
    {
        "id": "raid-alerts-2",
        "image": "/img/raid_alerts_2.png",
        "url": "https://air-alarms.in.ua/en",
    },
    {
        "id": "raid-alerts-5",
        "image": "/img/raid_alerts_5.png",
        "url": "https://alerts.na-agency.com/en/#stats",
    },
    {
        "id": "raid-alerts-3",
        "image": "/img/raid_alerts_3.png",
        "url": "https://github.com/Vadimkin/ukrainian-air-raid-sirens-dataset",
    },
    {
        "id": "raid-alerts-8",
        "image": "/img/raid_alerts_8.png",
        "url": "https://pulse.errata.page/en/alerts/",
    },
    {
        "id": "raid-alerts-9",
        "image": "/img/raid_alerts_9.png",
        "url": "https://uavradar.live/",
    },
]


@router.get("/content")
def content():
    """Provides the content for the Raid Alerts page."""
    return page_content
