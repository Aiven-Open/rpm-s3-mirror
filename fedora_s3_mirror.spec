Name:           fedora_s3_mirror
Version:        %{major_version}
Release:        %{minor_version}%{?dist}
Url:            http://github.com/aiven/fedora_s3_mirror
Summary:        Fedora S3 mirror tool
BuildArch:      noarch
License:        ASL 2.0
Source0:        fedora_s3_mirror-rpm-src.tar
BuildRequires:  python3-pytest
BuildRequires:  python3-yapf
BuildRequires:  rpm-build
Requires:       python3-defusedxml
Requires:       python3-requests
Requires:       python3-dateutil
Requires:       python3-boto3
Requires:       python3-lxml
Requires:       systemd

%undefine _missing_build_ids_terminate_build

%description
fedora_s3_mirror is a tool for syncing upstream Fedora repositories with S3


%global debug_package %{nil}


%prep
%setup -q -n fedora_s3_mirror


%build


%install
python3 setup.py install --prefix=%{_prefix} --root=%{buildroot}
sed -e "s@#!/bin/python@#!%{_bindir}/python@" -i %{buildroot}%{_bindir}/*
%{__install} -Dm0644 fedora_s3_mirror.unit %{buildroot}%{_unitdir}/fedora_s3_mirror.service


%check


%files
%defattr(-,root,root,-)
%doc LICENSE README.md fedora_s3_mirror.json
%{_bindir}/fedora_s3_mirror*
%{_unitdir}/fedora_s3_mirror.service
%{python3_sitelib}/*


%changelog
* Wed Mar 18 2020 William Coe <willcoe@aiven.io> - 1.0.0
- Initial version
