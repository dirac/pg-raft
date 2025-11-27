# Use the official Ubuntu 24.04 base image
FROM ubuntu:24.04

# Install OpenSSH server and necessary packages
RUN apt-get update && \
    apt-get install -y openssh-server sudo && \
    rm -rf /var/lib/apt/lists/*

RUN mkdir -p /run/sshd && chmod 0755 /run/sshd

RUN apt-get install -y build-essential vim
RUN apt-get install -y g++ unzip zip git curl gdb
RUN apt-get install -y clang-format clang-tidy


# Create a non-root user and set a password (replace 'youruser' and 'yourpassword')
# For production, consider using SSH keys instead of passwords.
RUN useradd -ms /bin/bash vmurty && \
    echo "vmurty:vmurty" | chpasswd && \
    adduser vmurty sudo

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
# $ ssh  vmurty@localhost -p 2222

# Install bazelisk
# $ wget https://github.com/bazelbuild/bazelisk/releases/download/v1.27.0/bazelisk-linux-arm64
# $ chmod ugo+x bazelisk-linux-arm64
# $ mv bazelisk-linux-arm64 bazelisk
# $ sudo mv bazelisk /usr/local/bin/.

