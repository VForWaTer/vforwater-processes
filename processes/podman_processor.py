import logging
from podman import PodmanClient


class CompletedContainer:
    def __init__(self, name, container_id, status, logs, exit_status=None, removed=False):
        self.name = name
        self.id = container_id
        self.status = status
        self.exit_status = exit_status
        self.removed = removed
        self._logs = tuple(logs)

    def reload(self):
        return None

    def logs(self, stream=False, *args, **kwargs):
        return iter(self._logs) if stream else list(self._logs)


class PodmanProcessor():
    DEFAULT_MOUNTS = [
        {'type': 'bind', 'source': '/lsdf', 'target': '/lsdf', 'read_only': True}
    ]

    def connect(uri='unix:///run/podman/podman.sock'):
        # Connect to Podman
        client = PodmanClient(base_url=uri)

        if not client.ping():
            logging.error("Podman service is NOT running")
            raise Exception("Podman service is NOT running")
        else:
            print("Podman service is running")
            logging.info("Podman service is running")
            # TODO: There is a bug in the following code
            # version = client.version()
            # print("Release: ", version["Version"])
            # logging.info("Release: ", version["Version"])
            # print("Compatible API: ", version["ApiVersion"])
            # logging.info("Compatible API: ", version["ApiVersion"])
            # print("Podman API: ", version["Components"][0]["Details"]["APIVersion"], "\n")
            # logging.info("Podman API: ", version["Components"][0]["Details"]["APIVersion"])

        return client

    @staticmethod
    def _with_default_mounts(mounts):
        effective_mounts = list(mounts or [])
        mounted_targets = {
            mount.get('target')
            for mount in effective_mounts
            if isinstance(mount, dict)
        }

        for mount in PodmanProcessor.DEFAULT_MOUNTS:
            if mount['target'] not in mounted_targets:
                effective_mounts.append(mount.copy())

        return effective_mounts

    @staticmethod
    def pull_run_image(client, image_name, container_name, environment=None, mounts=None, network_mode=None,
                       volumes=None, command=None):
        # Log available images (debug only) — guard so it never breaks runtime
        try:
            if hasattr(client, "images") and hasattr(client.images, "list"):
                logging.info(f"client.images.list() {client.images.list()}")
                logging.info("The following images are available: ")
                for i in client.images.list():
                    logging.info(f"Image ID: {i.id}, image name: {i.tags}")
            else:
                logging.warning(f"Client type has no images.list(): {type(client)}")
        except Exception as e:
            logging.warning(f"Skipping image listing (non-fatal): {e}")


        if not client.images.list(filters={"reference": image_name}):
            print(f"Pulling Podman image: {image_name}")
            logging.info(f"Pulling Podman image: {image_name}")
            client.images.pull(image_name)

        existing_container = client.containers.list(filters={"name": container_name})
        if existing_container:
            # print(f"Container '{container_name}' already exists. Removing...")
            logging.info(f"Container '{container_name}' already exists. Removing...")
            existing_container[0].stop()
            existing_container[0].remove(force=True)

        print(f"Running Podman container: {container_name}")
        logging.info(f"Running Podman container: {container_name}")
        effective_mounts = PodmanProcessor._with_default_mounts(mounts)
        logging.info(f"Using Podman mounts: {effective_mounts}")
        container = None
        container_id = None
        container_status = "unknown"
        exit_status = None
        logs = []
        removed = False
        result = None
        try:
            container = client.containers.run(
                image=image_name,
                detach=True,
                name=container_name,
                environment=environment,
                mounts=effective_mounts,
                network_mode=network_mode,
                # volumes=volumes,
                command=command,
                remove=False
            )
            container_id = getattr(container, "id", None)
            logging.info(f"Container to use: {container}")
        # except Exception as e:
        #     logging.info(f"Cannot run client.container. Error: {e}")

            # Start the container
            container.start()
            logging.info("Container started")

            # status of the container after starting
            container.reload()
            logging.info("Container reloaded")
            # print("container starting status :", container.status)
            logging.info(f"container starting status : {container.status}")

            # Print container logs
            # print(f"Container '{container.name}' logs:")
            logging.info(f" _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ Container '{container.name}' logs: _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ ")
            for line in container.logs(stream=True):
                # print(line.strip().decode('utf-8'))
                logs.append(line)
                logging.info(f" - - {line.decode('utf-8')} - - ")
            logging.info(" _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ finished logs _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ ")

            # exit status code
            exit_status = container.wait()
            # print("exit_status :", exit_status)
            logging.info(f"exit_status : {exit_status}")

            # status of the container
            container.reload()
            container_status = container.status
            print("container  exiting status :", container.status)
            logging.info(f"container  exiting status : {container.status}")

            result = CompletedContainer(
                name=container_name,
                container_id=container_id,
                status=container_status,
                logs=logs,
                exit_status=exit_status,
                removed=False
            )
            return result
        # return {
        #     "container": container,
        #     "container_status": container.status
        # }
        except Exception as e:
            logging.error(f"Cannot run client.container. Error: {e}")
            raise
        finally:
            if container is not None:
                try:
                    container.remove(force=True)
                    removed = True
                    logging.info(f"Removed Podman container: {container_name}")
                except Exception as cleanup_error:
                    logging.warning(f"Could not remove Podman container '{container_name}': {cleanup_error}")
                if result is not None:
                    result.removed = removed
        
    def get_secrets(file_name="processes/secret.txt"):

        secrets = {}
        with open(file_name, 'r') as f:
            lines = f.readlines()
            for line in lines:
                key, value = line.strip().split('=')
                secrets[key] = value
        return secrets
