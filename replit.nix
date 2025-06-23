{ pkgs }: {
  deps = [
    pkgs.glibcLocales
    pkgs.zlib
    pkgs.xcodebuild
    pkgs.python311Full
    pkgs.python311Packages.flask
    pkgs.python311Packages.requests
    pkgs.python311Packages.pandas
    pkgs.python311Packages.pdfplumber
  ];
}
