bind = "0.0.0.0:8080"
worker_class = "uvicorn.workers.UvicornWorker"
workers = 5
loglevel = "info"
accesslog = "access_gamma_endpoint.log"
# acceslogformat = "%(t)s %(l)s %(h)s %(l)s %({cf-connecting-ip}i)s %(l)s %(u)s %(t)s %(r)s %(s)s"
# errorlog = "error_gamma_endpoint.log"
timeout = 65
