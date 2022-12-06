Name:           rpm_s3_mirror
Version:        %{major_version}
Release:        %{minor_version}%{?dist}
Url:            http://github.com/aiven/rpm-s3-mirror
Summary:        Aiven RPM S3 mirror tool
BuildArch:      noarch
License:        ASL 2.0
Source0:        rpm_s3_mirror-rpm-src.tar
BuildRequires:  python3-pytest
BuildRequires:  rpm-build
Requires:       python3-defusedxml
Requires:       python3-requests
Requires:       python3-dateutil
Requires:       python3-botocore
Requires:       python3-lxml
Requires:       systemd
Requires:       zchunk

%undefine _missing_build_ids_terminate_build

%description
rpm_s3_mirror is a tool to create a RPM mirror in s3 and periodically sync changes


%global debug_package %{nil}


%prep
%setup -q -n rpm_s3_mirror


%build


%install
python3 setup.py install --prefix=%{_prefix} --root=%{buildroot}
sed -e "s@#!/bin/python@#!%{_bindir}/python@" -i %{buildroot}%{_bindir}/*
%{__install} -Dm0644 rpm_s3_mirror.unit %{buildroot}%{_unitdir}/rpm_s3_mirror.service


%check


%files
%defattr(-,root,root,-)
%doc LICENSE README.md rpm_s3_mirror.json
%{_bindir}/rpm_s3_mirror*
%{_unitdir}/rpm_s3_mirror.service
%{python3_sitelib}/*


%changelog
* Wed Mar 18 2020 William Coe <willcoe@aiven.io> - 1.0.0
- Initial version
