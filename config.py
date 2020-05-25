__all__ = (
    'DATABASES',
    'METROBUSES_API_URL',
)

DATABASES = {
    'metrobuses':{
        'NAME': 'buses',
        'USER': 'django',
        'PASSWORD': '12345678',
        'HOST': '0.0.0.0',
        'PORT': 5433,
    },
}

METROBUSES_API_URL = 'https://datos.cdmx.gob.mx/api/records/1.0/search/?dataset=prueba_fetchdata_metrobus&q='
