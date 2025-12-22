# Use the official Ubuntu 24.04 base image
FROM ubuntu:24.04

# Install OpenSSH server and necessary packages
RUN apt-get update && \
    apt-get install -y openssh-server sudo && \
    rm -rf /var/lib/apt/lists/*

RUN mkdir -p /run/sshd && chmod 0755 /run/sshd

RUN apt-get update
RUN apt-get upgrade

RUN apt-get install -y build-essential vim
RUN apt-get install -y g++ unzip zip git curl gdb
RUN apt-get install -y clang-format clang-tidy
RUN apt-get install -y libfuse3-dev

RUN addgroup fuse

# Create a non-root user and set a password (replace 'youruser' and 'yourpassword')
# For production, consider using SSH keys instead of passwords.
RUN useradd -g fuse -ms /bin/bash postgres && \
    echo "postgres:postgres" | chpasswd && \
    adduser postgres sudo

RUN wget https://github.com/bazelbuild/bazelisk/releases/download/v1.27.0/bazelisk-linux-arm64
RUN chmod ugo+x bazelisk-linux-arm64
RUN mv bazelisk-linux-arm64 bazelisk
RUN mv bazelisk /usr/local/bin/.

# postgresql installation (https://wiki.postgresql.org/wiki/Apt)
RUN apt-get install -y postgresql-common ca-certificates
RUN /usr/share/postgresql-common/pgdg/apt.postgresql.org.sh -y
RUN apt-get update

# Install postgresql (/usr/lib/postgresql/18/)
RUN apt-get install postgresql-18

# Allow password authentication for SSH (optional, for development/testing)
# In production, consider disabling password authentication and using SSH keys.
RUN sed -i 's/#PermitRootLogin prohibit-password/PermitRootLogin yes/' /etc/ssh/sshd_config && \
    sed -i 's/PermitRootLogin prohibit-password/PermitRootLogin yes/' /etc/ssh/sshd_config && \
    sed -i 's/#PasswordAuthentication yes/PasswordAuthentication yes/' /etc/ssh/sshd_config

# Expose the SSH port
EXPOSE 22

# Start the SSH service when the container starts
CMD ["/usr/sbin/sshd", "-D"]

# Create Image
# $ docker build -t ubuntu .

# Start the server
# $ docker run -d -p 2222:22 --name dev ubuntu

# Login
# $ ssh postgres@localhost -p 2222

# Create ssh key for git
# $ ssh-keygen -t ed25519 -C "venkat.murty@gmail.com"
# $ eval "$(ssh-agent -s)"
# $ ssh-add ~/.ssh/id_ed25519
# $  cat ~/.ssh/id_ed25519.pub
# $  git clone git@github.com:dirac/pg-raft.git

