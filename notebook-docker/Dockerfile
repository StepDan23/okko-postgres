FROM jupyter/scipy-notebook

WORKDIR /home/$NB_USER/work/

ADD requirements.txt build/

RUN pip install --upgrade pip \
    && pip install --no-cache-dir -r build/requirements.txt
