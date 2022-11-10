%define debug_package   %{nil}

%global provider        github
%global provider_tld    com
%global project         shatteredsilicon
%global repo            mtuned
%global provider_prefix	%{provider}.%{provider_tld}/%{project}/%{repo}
%global user            mtuned
%global group           mtuned
%global uid             73
%global gid             73

Name:       %{repo}
Version:    %{_version}
Release:    1%{?dist}
Summary:    MySQL/MariaDB Tuning Daemon
License:    GPLv3
URL:        https://%{provider_prefix}

Source0:    %{name}-%{version}.tar.gz

BuildRequires:	    golang >= 1.11, systemd
Requires:           smartmontools, sysstat
Requires(pre):      shadow-utils
Requires(post):     systemd
Requires(preun):    systemd
Requires(postun):   systemd

%description
mtuned is a standalone daemon that monitors settings and statuses and
identifies configuration improvements.

%prep
%setup -q -n %{repo}

%build
export GO111MODULE=on
go build -ldflags="-s -w -X '%{name}/pkg/util.Version=%{version}'" ./cmd/mtuned

%install
install -d -p %{buildroot}%{_sbindir}
install -d %{buildroot}/usr/lib/systemd/system
install -d %{buildroot}%{_sysconfdir}
install -d %{buildroot}/var/log
install -p -m 0755 mtuned %{buildroot}%{_sbindir}/mtuned
install -p -m 0644 %{name}.service %{buildroot}/usr/lib/systemd/system/%{name}.service
install -p -m 0644 %{name}.cnf.example %{buildroot}%{_sysconfdir}/%{name}.cnf
touch %{buildroot}/var/log/%{name}.log

%pre
getent group %{group} >/dev/null || groupadd -f -g %{gid} -r %{user}
if ! getent passwd %{user} >/dev/null ; then
    if ! getent passwd %{uid} >/dev/null ; then
      useradd -r -u %{uid} -g %{group} -s /sbin/nologin -c "Mtuned Service User" %{user}
    else
      useradd -r -g %{group} -s /sbin/nologin -c "Mtuned Service User" %{user}
    fi
fi
exit 0

%post
password=$(dd if=/dev/urandom bs=1024 count=1 | md5sum | cut -d' ' -f1)
sed -i "/^password /s/=.*$/= ${password}/" %{_sysconfdir}/%{name}.cnf

echo "

Please run below commands in MySQL/MariaDB to create the user if it doesn't exist:

  CREATE USER mtuned@localhost IDENTIFIED BY '${password}';
  GRANT ALL ON *.* TO mtuned@localhost;
  FLUSH PRIVILEGES;

If the 'mtuned' user exists, please use below commands to update the password:

  ALTER USER mtuned@localhost IDENTIFIED BY '${password}';
  FLUSH PRIVILEGES;

And make sure user 'mtuned' has right permission to read/write below files:
- 'persistently tuned' config file (default is /etc/my.cnf.d/zz-tuned.cnf)
- /sys/block/<dev>/queue/scheduler (if bold=1)
- /sys/kernel/mm/transparent_hugepage/enabled (if bold=1)
- /dev/<dev> (if autodetect ssd type)
"

%systemd_post %{name}.service

%preun
%systemd_preun %{name}.service

%postun
%systemd_postun %{name}.service

%files
%license LICENSE
%doc README.md
%{_sbindir}/mtuned
/usr/lib/systemd/system/%{name}.service
%{_sysconfdir}/%{name}.cnf
%attr(0644, mtuned, mtuned) /var/log/%{name}.log
