from environs import Env

__all__ = (
    'DATABASES',
    'METROBUSES_API_URL',
)


env = Env()
env.read_env()

DATABASES = {
    'metrobuses':{
        'NAME': env('DB_NAME'),
        'USER': env('DB_USER'),
        'PASSWORD': env('DB_PASSWORD'),
        'HOST': env('DB_HOST'),
        'PORT': env('DB_PORT'),
    },
}

METROBUSES_API_URL = env('METROBUSES_API_URL')

BROKER = env('BROKER')
