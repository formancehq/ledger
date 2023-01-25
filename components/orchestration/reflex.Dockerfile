FROM golang:1.18-alpine
RUN go install github.com/cespare/reflex@latest
