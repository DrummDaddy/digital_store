INSERT INTO products (
    sku,
    name,
    product_type,
    price_minor,
    currency,
    image,
    active
)
VALUES
    (
        'STEAM-TOPUP-500',
        'Пополнение Steam 500 ₽',
        'topup',
        500,
        'RUB',
        'assets/steam.png',
        TRUE
    ),
    (
        'STEAM-TOPUP-1000',
        'Пополнение Steam 1000 ₽',
        'topup',
        1000,
        'RUB',
        'assets/steam.png',
        TRUE
    ),
    (
        'STEAM-TOPUP-2500',
        'Пополнение Steam 2500 ₽',
        'topup',
        2500,
        'RUB',
        'assets/steam.png',
        TRUE
    ),
    (
        'KEY-CS2-PRIME',
        'CS2 Prime Status ключ',
        'key',
        1290,
        'RUB',
        'assets/cs2.png',
        TRUE
    ),
    (
        'KEY-GTA5',
        'GTA V ключ активации',
        'key',
        1990,
        'RUB',
        'assets/gta5.png',
        TRUE
    ),
    (
        'KEY-EFT',
        'Escape from Tarkov ключ',
        'key',
        3490,
        'RUB',
        'assets/eft.png',
        TRUE
    ),
    (
        'SUB-DISCORD-1M',
        'Discord Nitro 1 месяц',
        'subscription',
        399,
        'RUB',
        'assets/discord.png',
        TRUE
    ),
    (
        'SUB-YT-3M',
        'YouTube Premium 3 месяца',
        'subscription',
        1490,
        'RUB',
        'assets/youtube.png',
        TRUE
    ),
    (
        'SUB-SPOTIFY-1M',
        'Spotify Premium 1 месяц',
        'subscription',
        299,
        'RUB',
        'assets/spotify.png',
        TRUE
    ),
    (
        'GIFT-PSN-1000',
        'PlayStation Store карта 1000 ₽',
        'giftcard',
        1000,
        'RUB',
        'assets/psn.png',
        TRUE
    ),
    (
        'GIFT-XBOX-1500',
        'Xbox Gift Card 1500 ₽',
        'giftcard',
        1500,
        'RUB',
        'assets/xbox.png',
        TRUE
    ),
    (
        'GIFT-ROBLOX-800',
        'Roblox 800 Robux',
        'giftcard',
        890,
        'RUB',
        'assets/roblox.png',
        TRUE
    )
ON CONFLICT (sku) DO NOTHING;

INSERT INTO inventory (
    sku,
    available,
    reserved
)
SELECT
    sku,
    10,
    0
FROM products
ON CONFLICT (sku) DO NOTHING;