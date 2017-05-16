

import os

DATABASES = {
            'default': {
            'ENGINE': 'django_redshift_backend',
                'OPTIONS': {
                    'options': '-c search_path=public'
            },
            'NAME': 'email_data',
            'USER': 'xxxxx',
            'PASSWORD': 'xxxxx',
            'HOST': 'xxxxx.redshift.amazonaws.com',
            'PORT': '5439',
        }
    }

INSTALLED_APPS = (
    'activity',
    )

SECRET_KEY = 'random'




