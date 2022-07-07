FROM public.ecr.aws/lambda/python:3.8

RUN echo creating AWS lambda image


RUN python3 -m pip && \
    pip install voluptuous


COPY ./shared_workspace/app.py   ./
CMD ["app.handler"]