rm -rf publish/
dotnet publish -o "publish/" -r "win-x64" -c Release --no-self-contained -p:PublishSingleFile=true
zip -rjm -9 publish/es_to_sql.exe.zip  publish/*
