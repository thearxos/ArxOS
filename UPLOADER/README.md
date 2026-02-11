## BUILD TOOL
go build -o RxArchiver main.go

## GIVE EXECUTION PERMISSION & RUN
chmod +x RxArchiver && ./RxArchiver

## Quick recap of what it does (USAGE):

```bash
rxarchiver login — saves archive.org S3 keys to ~/.rxarchiver/config.json (0600 perms)

rxarchiver upload -f file.iso -i arxos-mars --hash — single file upload with SHA256/MD5

rxarchiver upload -d ./release/ -i arxos-mars -w 8 --hash — directory upload, 8 concurrent workers

rxarchiver hash -f file.iso — hash-only mode, no upload
```
