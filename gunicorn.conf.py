bind = "0.0.0.0:8080"
worker_class = "uvicorn.workers.UvicornWorker"
workers = 5
loglevel = "info"
accesslog = "/var/log/gunicorn/access_log_gamma_endpoint"
acceslogformat = "%(h)s %(l)s %(u)s %(t)s %(r)s %(s)s %(b)s %(f)s %(a)s"
errorlog = "/var/log/gunicorn/error_gamma_endpoint"
timeout = 65
