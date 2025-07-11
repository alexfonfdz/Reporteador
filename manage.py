#!/usr/bin/env python
"""
Copyright (c) 2019 - present AppSeed.us
"""

import os
import sys

def main():
    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'core.settings')
    try:
        from django.core.management import execute_from_command_line
    except ImportError as exc:
        raise ImportError(
            "Couldn't import Django. Are you sure it's installed and "
            "available on your PYTHONPATH environment variable? Did you "
            "forget to activate a virtual environment?"
        ) from exc
    
    if sys.argv[1] == "cron":
        from apps.home.cron_job import main as main_cron
        import asyncio
        asyncio.run(main_cron())
        return
        
    execute_from_command_line(sys.argv)

if __name__ == '__main__':
    main()
