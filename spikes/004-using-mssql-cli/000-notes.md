https://github.com/dbcli/mssql-cli seems like a useful tool for
interacting with mssql instances from a linux machine.

I was able to install it from microsoft's apt repo.

```
vagrant@taps-tvisher1:~$ apt-cache policy mssql-cli
mssql-cli:
  Installed: 0.15.0-1
  Candidate: 0.15.0-1
  Version table:
 *** 0.15.0-1 500
        500 https://packages.microsoft.com/ubuntu/16.04/prod xenial/main amd64 Packages
        100 /var/lib/dpkg/status
     0.14.0-1 500
        500 https://packages.microsoft.com/ubuntu/16.04/prod xenial/main amd64 Packages
     0.13.0-1 500
        500 https://packages.microsoft.com/ubuntu/16.04/prod xenial/main amd64 Packages
     0.12.0-1 500
        500 https://packages.microsoft.com/ubuntu/16.04/prod xenial/main amd64 Packages
     0.11.0-1 500
        500 https://packages.microsoft.com/ubuntu/16.04/prod xenial/main amd64 Packages
     0.10.0.dev1804041738-1 500
        500 https://packages.microsoft.com/ubuntu/16.04/prod xenial/main amd64 Packages
     0.10.0-1 500
        500 https://packages.microsoft.com/ubuntu/16.04/prod xenial/main amd64 Packages
```

From https://github.com/dbcli/mssql-cli/blob/master/doc/installation/linux.md#ubuntu-1604

```
# Import the public repository GPG keys
wget -qO- https://packages.microsoft.com/keys/microsoft.asc | sudo apt-key add -

# Register the Microsoft Ubuntu repository
sudo curl -o /etc/apt/sources.list.d/microsoft.list "https://packages.microsoft.com/config/ubuntu/$(lsb_release -sr)/prod.list"

# Update the list of products
sudo apt-get update

# Install mssql-cli
sudo apt-get install mssql-cli

# Start mssql-cli
mssql-cli
```
