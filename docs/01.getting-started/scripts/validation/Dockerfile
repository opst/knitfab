FROM python:3.11

WORKDIR /work

RUN pip install numpy==1.26.4 && \
    pip install torch==2.2.1 --index-url https://download.pytorch.org/whl/cpu

COPY . .

ENTRYPOINT [ "python", "-u", "validation.py", "--dataset", "/in/dataset", "--model", "/in/model/model.pth" ]
