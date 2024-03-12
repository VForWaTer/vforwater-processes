from podman import PodmanClient

class PodmanProcessor():

    def connect(uri='unix:///run/podman/podman.sock'):
        # Connect to Podman
        client = PodmanClient(base_url=uri)

        if not client.ping():
            raise Exception("Podman service is not running")
        else:
            print("Podman service is running")
            version = client.version()
            print("Release: ", version["Version"])
            print("Compatible API: ", version["ApiVersion"])
            print("Podman API: ", version["Components"][0]["Details"]["APIVersion"], "\n")

        return client

    def pull_run_image(client, image_name, container_name, environment=None, mounts=None, network_mode=None, volumes=None, command=None):
        # Pull the Docker image
        for image in client.images.list():
            print('image list: ', image, image.id, "\n")
            if image_name not in image.labels['org.opencontainers.image.title']:
                print(f"Pulling Podman image: {image_name}")
                image = client.images.pull(image_name)

        # Check if container with the same name exists, and remove it if it does
        if container_name in client.containers.list():
            print(f"Container '{container_name}' already exists. Removing...")
            client.containers.get(container_name).remove(force=True)

        # Run the Docker container
        print(f"Running Podman container: {container_name}")
        container = client.containers.create(
            image=image,
            detach=False,
            name=container_name,
            environment=environment,
            mounts=mounts,
            network_mode=network_mode,
            # volumes=volumes,
            # command=command,
            remove=False
        )

        # Start the container
        container.start()

        # Print container logs
        print(f"Container '{container.name}' logs:")
        for line in container.logs(stream=True):
            print(line.strip().decode('utf-8'))

        # # Stop the container
        # if container.status != 'exited':
        #     print(f"Container status with ID '{container.name}' is '{container.status}'")
        #     container.stop()

        return container

    def get_secrets(file_name="secret.txt"):
        secrets = {}
        with open(file_name, 'r') as f:
            lines = f.readlines()
            for line in lines:
                key, value = line.strip().split('=')
                secrets[key] = value
        return secrets
