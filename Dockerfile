FROM python:3.10.9

RUN useradd --create-home geoapi

COPY ./ /home/geoapi
WORKDIR /home/geoapi
RUN chown -R geoapi:geoapi /home/geoapi

RUN pip install --upgrade pip &&  \
    pip install -r requirements.txt

USER geoapi
ENV PYGEOAPI_CONFIG=pygeoapi-config.yml

CMD [ "python", "-m" , "flask", "run", "--host=0.0.0.0"]