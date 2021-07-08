#!/usr/bin/env python3
from os import environ as ENV
import sys
import boto3
import json
import subprocess


class bcolors:
    HEADER = "\033[95m"
    OKBLUE = "\033[94m"
    OKCYAN = "\033[96m"
    OKGREEN = "\033[92m"
    WARNING = "\033[93m"
    FAIL = "\033[91m"
    ENDC = "\033[0m"
    BOLD = "\033[1m"
    UNDERLINE = "\033[4m"


def get_ecs_service(cluster_name, tag):
    ecs = boto3.client("ecs")

    # Get Latest TaskDef
    ecs_services = ecs.list_services(
        cluster=cluster_name,
    )["serviceArns"]

    for svc in ecs_services:
        service_name = svc.replace(
            "arn:aws:ecs:eu-west-1:496288716344:service/", ""
        ).replace(cluster_name + "/", "")
        print(f"{bcolors.OKGREEN}: Start to deploy {service_name} with {tag}")
        process = subprocess.Popen(f"ecs deploy {cluster_name} {service_name} --tag {tag}", shell=True, stdout=subprocess.PIPE)
        process.wait()

    return "true"


if __name__ == "__main__":
    get_ecs_service(sys.argv[1], sys.argv[2])
