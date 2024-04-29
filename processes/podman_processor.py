import logging
from podman import PodmanClient

class PodmanProcessor():

    def connect(uri='unix:///run/podman/podman.sock'):
        # Connect to Podman
        client = PodmanClient(base_url=uri)

        if not client.ping():
            logging.error("Podman service is NOT running")
            raise Exception("Podman service is NOT running")
        else:
            print("Podman service is running")
            logging.info("Podman service is running")
            # TODO: There is a bug in the following code. Fix it
            # version = client.version()
            # print("Release: ", version["Version"])
            # logging.info("Release: ", version["Version"])
            # print("Compatible API: ", version["ApiVersion"])
            # logging.info("Compatible API: ", version["ApiVersion"])
            # print("Podman API: ", version["Components"][0]["Details"]["APIVersion"], "\n")
            # logging.info("Podman API: ", version["Components"][0]["Details"]["APIVersion"])

        return client

    def pull_run_image(client, image_name, container_name, environment=None, mounts=None, network_mode=None,
                       volumes=None, command=None):
        secrets = PodmanProcessor.get_secrets()
        # Log available Docker image
        logging.info(f"client.images.list() {client.images.list()}")
        # logging.info("The following images are available: ")
        # for i in client.images.list():
        #     logging.info(f"Image ID: {i.id}, image name: {i.name}")

        # Pull the Docker image
        # print("image: ", client.images.list(filters={"reference": image_name}))
        # logging.info("image: ", client.images.list(filters={"reference": image_name}))
        # if not client.images.list(filters={"reference": image_name}):
        #     print(f"Pulling Podman image: {image_name}")
        #     logging.info(f"Pulling Podman image: {image_name}")
        #     client.images.pull(image_name)

        existing_container = client.containers.list(filters={"name": container_name})
        if existing_container:
            # print(f"Container '{container_name}' already exists. Removing...")
            logging.info(f"Container '{container_name}' already exists. Removing...")
            existing_container[0].stop()
            existing_container[0].remove(force=True)

        print(f"Running Podman container: {container_name}")
        logging.info(f"Running Podman container: {container_name}")
        try:
            container = client.containers.run(
                image=image_name,
                detach=True,
                name=container_name,
                environment=environment,
                mounts=mounts,
                network_mode=network_mode,
                # volumes=volumes,
                command=command,
                remove=False
            )
            logging.info(f"Container to use: {container}")
        except Exception as e:
            logging.info(f"Cannot run client.container. Error: {e}")

        # Start the container
        container.start()
        logging.info("Container started")

        # status of the container after starting
        container.reload()
        logging.info("Container reloaded")
        # print("container starting status :", container.status)
        logging.info("container starting status :", container.status)

        # Print container logs
        # print(f"Container '{container.name}' logs:")
        logging.info(f" _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ Container '{container.name}' logs: _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ ")
        for line in container.logs(stream=True):
            # print(line.strip().decode('utf-8'))
            logging.info(f" - - {line.decode('utf-8')} - - ")
        logging.info(" _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ finished logs _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ ")

        # exit status code
        exit_status = container.wait()
        # print("exit_status :", exit_status)
        logging.info(f"exit_status : {exit_status}")

        # status of the container
        container.reload()
        print("container  exiting status :", container.status)
        logging.info(f"container  exiting status : {container.status}")

        return container
        # return {
        #     "container": container,
        #     "container_status": container.status
        # }

    def get_secrets(file_name="processes/secret.txt"):

        secrets = {}
        with open(file_name, 'r') as f:
            lines = f.readlines()
            for line in lines:
                key, value = line.strip().split('=')
                secrets[key] = value
        return secrets
