FROM alpine:latest
RUN apk add --no-cache gcc musl-dev
WORKDIR /app
COPY neighbor.c .
RUN gcc -o neighbor neighbor.c -static
RUN cp neighbor /neighbor && rm -rf /app
RUN apk del gcc musl-dev
WORKDIR /
CMD ["sleep", "infinity"]
 
