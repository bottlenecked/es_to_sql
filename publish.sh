rm -rf publish/*
dotnet publish -o "publish/" -r "win-x64" -c Debug --no-self-contained -p:PublishSingleFile=true
zip -rjm -9 publish/es_to_sql.zip  publish/*
