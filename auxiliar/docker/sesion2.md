# Infraestructura en Docker

Además de tener la capacidad de correr piezas de _software_ específico en un ambiente aislado, docker puede ser usado para levantar una infraestructura completa. Esto se logra comunicando contenedores a través de una red virtual y/o compartiendo volúmenes entre ellos. Este tipo de infraestructura, dónde cada parte está aislada de las demás, es ideal para una arquitectura de microservicios.

## Volúmenes

Docker tiene la capacidad de crear y manejar volúmenes virtuales, permitiendo una especie de "directorio compartido" entre contenedores, e incluso con el anfitrión.

Aunque en un Dockerfile se puede utilizar la instrucción VOLUME, esto **crea un volumen virtual nuevo** en el cual puede escribir la aplicación. El uso de esta instrucción es limitado, pues es difícilmente accesible desde el sistema anfitrión. El principal uso de la instrucción VOLUME  es para aplicaciones que escriben frecuentemente a disco, pues debido a la virtualización, es más veloz escribir a una carpeta declarada como VOLUME que a una carpeta virtual dentro del contenedor.

La principal manera de compartir archivos entre contenedores o hacia el contenedor es a través de el parámetro `-v` o `--volume`, así como `docker volume`.

Por ejemplo, la instrucción `docker run -v <host_path>:<container_path> <image>` utilizará el _path_ absoluto `<host_path>` para montar un volumen virtual en el contenedor en el punto de montaje `<container_path>`. Esto quiere decir que cualquier archivo que se encuentre en `<host_path>` será visible al contenedor y que cualquier archivo escrito por el contenedor en `<container_path>` será visible para el host.

También podemos crear un volumen antes de correr la imagen con `docker volume create` Esto nos permite especificar opciones de montaje, como el _driver_ utilizado. Un caso de uso de esto es utilizar un volumen remoto vía `ssh`.

Un ejemplo del uso de  `docker volume`:

```
docker volume create myvol
docker run -v myvol:<container_path> <image>
```

Otros subcomandos de `docker volume` nos permiten administrar estos volúmenes, ver `docker volume --help` para los detalles.

## Redes

Por defecto, docker conecta los contenedores nuevos a una red virtual llamada `bridge`. Aunque los contenedores en esta red pueden comunicarse vía IP, no existe servicio de descrubimiento de nombres en `bridge`, por lo que es mejor utilizar una red virtual creada por el usuario. 

Por ejemplo, al correr los siguientes comandos, el segundo contenedor (`container2`) será capaz de "ver" al primer contenedor (`container1`) en el puerto 80

```
docker network create mynet
docker run --network mynet --name=container1 -p 80 nginx
docker run -it --network mynet --name=container2 ubuntu bash
```

Si instalamos `ping` en el segundo contenedor deberíamos de ser capaces de ver el primero:

```
# apt-get update; apt-get install -y iputils-ping
# ping container1
PING container1 (172.19.0.3) 56(84) bytes of data.
64 bytes from container1.mynet (172.19.0.3): icmp_seq=1 ttl=64 time=0.074 ms
64 bytes from container1.mynet (172.19.0.3): icmp_seq=2 ttl=64 time=0.050 ms
64 bytes from container1.mynet (172.19.0.3): icmp_seq=3 ttl=64 time=0.056 ms
```

## Compose

Aunque las opciones de volúmenes y redes explicadas son muy flexibles, no son una manera práctica de especificar una infraestructura con varios servicios, bases de datos, etc. Para eso existe `docker compose`.

Compose nos permite especificar, en un archivo YAML (`docker-compose.yaml`), los servicios que queremos para nuestra aplicación, especificando puertos, redes, opciones de configuración, argumentos, etc.

Un ejemplo del archivo `docker-compose.yaml`:

```
version: 3
services:
  postgres:
      image: postgres:9.6
      environment:
        - POSTGRES_USER=arinarmo
        - POSTGRES_PASSWORD=badpass
        - POSTGRES_DB=mydb
      ports:
        - 5432:5432
  main:
      build: 
      	context: ./main_app
  	  entrypoint: /code/main.py
  	  environment:
  	    - DB_USER=arinarmo
  	    - DB_PASS=badpass
  	    - DB_NAME=mydb
  	    - DB_PORT=5432
	    
```

En este archivo especificamos:

* Una base de datos usando la imagen oficial de PostgreSQL 9.6, especificando las credenciales a utilizar. El puerto expuesto nos permite conectarnos desde el host u otros contenedores **en la misma red.**
* Una aplicación que se construye a partir de un Dockerfile ubicado en `./main_app`, especificando variables de ambiente que se usarán en el script para conectarnos a nuestra base de datos y un  _entrypoint_.

La referencia completa para el YAML de compose se encuentra [aquí](https://docs.docker.com/compose/compose-file/), a continuación revisamos algunas de las secciones más importantes

### services

Aquí especificamos los servicios (contenedores o grupos de contenedores) que necesitamos que se "levanten" al iniciar la infraestructura. Por defecto estos servicios se levantarán una vez que terminen, ya sea por fallo o terminación exitosa.

Dentro de **services** existen varias claúsulas importantes:

* **build**: Nos permite especificar parámetros de construcción de la imagen, como el contexto (directorio) en dónde se construye, argumentos para el dockerfile, o incluso un dockerfile alternativo. Normalmente se usa **build** o **image**, pero no los dos.

* **image**: Especifica la imagen a utilizar para correr el servicio, puede existir en un registry remoto como DockerHub o en nuestra máquina local.

* **networks**: Redes virtuales a las cuales se debe unir el servicio. Estas redes deben estar definidas en el YAML

* **ports**: Especifica los puertos a "abrir" a la red virtual en la que se encuentre el servicio. Se puede también especificar un túnel para que sean accesibles desde el anfitrión.

* **volumes**: Volúmenes que deben estar disponibles. Se pueden definir en esta cláusula o en otra sección del YAML

* **deploy**: Nos permite configurar parámetros de despliegue del servicio, como el número de réplicas, el límite máximo de recursos a utilizar (por ejemplo, para evitar la canibalización) y la política de reinicio del servicio

* **depends_on**: Especifica que otros servicios del YAML necesitan ser levantados antes del actual. Esto **no espera a que esos servicios se encuentren "listos"**, sólo espera a que sean levantados (scripts específicos se necesitan para eso)

* **entrypoint**: El script o comando a correr al levantar este servicio

* **environment**: Variables de ambiente. Útil para especificar credenciales y otras configuraciones efímeras

  ​

## Ejemplo completo

En la carpeta `compose` se encuentra un ejemplo completo. Usa `docker-compose up` y abre [este link](http://localhost:8080) en una pestaña de navegador. También puedes ver la base de datos de grafo neo4j en [este link](http://localhost:7474).