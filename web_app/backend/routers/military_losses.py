from fastapi import APIRouter

router = APIRouter(prefix="/api/military-losses", tags=["military-losses"])


page_content = [
    {
        "id": "military_losses_1",
        "image": "https://storage.googleapis.com/kaggle-datasets-images/1967621/3472411/2b2c4ecb4946701c3876d29c7923ebb1/dataset-cover.png?t=2022-04-16-14-18-08",
        "url": "https://www.kaggle.com/datasets/piterfm/2022-ukraine-russian-war",
    },
    {
        "id": "military_losses_2",
        "image": "/img/military_losses_2.png",
        "url": "https://ualosses.org/en/soldiers/",
    },
    {
        "id": "military_losses_3",
        "image": "/img/military_losses_3.png",
        "url": "https://www.ukrainewarlosses.com/",
    },
    {
        "id": "military_losses_4",
        "image": "/img/military_losses_4.png",
        "url": "https://russian-casualties.in.ua/",
    },
    {
        "id": "military_losses_5",
        "image": "/img/military_losses_5.png",
        "url": "https://ukr.warspotting.net/",
    },
]


@router.get("/content")
def content():
    """
    Get the content for the military losses page.
    """
    return page_content
