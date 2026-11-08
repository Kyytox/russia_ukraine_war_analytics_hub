from fastapi import APIRouter

router = APIRouter(
    prefix="/api/incidents-railways/informations",
    tags=["incidents-railways-informations"],
)


@router.get("/data")
def get_informations():
    """Return only the source links (Twitter/X accounts and Telegram channels).

    All descriptive / methodology text lives in the frontend i18n files
    (frontend/src/i18n/<lang>/incidents-railways.json), not in the backend.
    """
    return {
        "twitter_accounts": [
            "https://x.com/LXSummer1",
            "https://x.com/Schizointel",
            "https://x.com/Prune602",
            "https://x.com/NOELreports",
            "https://x.com/igorsushko",
        ],
        "telegram_channels": [
            "https://t.me/electrichki",
            "https://t.me/astrapress",
            "https://t.me/shot_shot/",
            "https://t.me/atesh_ua",
            "https://t.me/legionoffreedom",
            "https://t.me/VDlegionoffreedom",
            "https://t.me/ENews112",
            "https://t.me/telerzd",
            "https://t.me/mzd_rzd",
            "https://t.me/magistral_kuvalda",
            "https://t.me/nskzd",
            "https://t.me/news_zszd",
            "https://t.me/D4msk",
            "https://t.me/rospartizan",
            "https://t.me/boakom",
            "https://t.me/rdpsru",
            "https://t.me/ostanovi_vagony_2023",
            "https://t.me/activatica",
            "https://t.me/russvolcorps",
        ],
    }
