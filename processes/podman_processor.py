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
        print("image: ", client.images.list(filters={"reference": image_name}))
        if not client.images.list(filters={"reference": image_name}):
            print(f"Pulling Podman image: {image_name}")
            client.images.pull(image_name)

        existing_container = client.containers.list(filters={"name": container_name})
        if existing_container:
            print(f"Container '{container_name}' already exists. Removing...")
            existing_container[0].remove(force=True)

        print(f"Running Podman container: {container_name}")
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

        # Start the container
        container.start()

        # status of the container after starting
        container.reload()
        print("container starting status :", container.status)

        # exit status code 
        exit_status = container.wait()  
        print("exit_status :", exit_status)

        # status of the container 
        container.reload()
        print("container  exiting status :", container.status)


        # Print container logs
        print(f"Container '{container.name}' logs:")
        for line in container.logs(stream=True):
            print(line.strip().decode('utf-8'))

        return {
            "container" : container,
            "container_status": container.status
        }

    def get_secrets(file_name="processes/secret.txt"):

        secrets = {}
        with open(file_name, 'r') as f:
            lines = f.readlines()
            for line in lines:
                key, value = line.strip().split('=')
                secrets[key] = value
        return secrets
