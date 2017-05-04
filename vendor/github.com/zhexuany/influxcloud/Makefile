PACKAGES=$(find . -name '*.go' | grep -vE 'vendor')

default:

metalint: deadcode cyclo aligncheck defercheck structcheck lint errcheck

deadcode:
	@deadcode $(PACKAGES) 2>&1

cyclo:
	@gocyclo -over 10 $(PACKAGES)

aligncheck:
	@aligncheck $(PACKAGES)

defercheck:
	@defercheck $(PACKAGES)


structcheck:
	@structcheck $(PACKAGES)

lint:
	@for pkg in $(PACKAGES); do golint $$pkg; done

errcheck:
	@for pkg in $(PACKAGES); do \
	  errcheck -ignorepkg=bytes,fmt -ignore=":(Rollback|Close)" $$pkg \
	done

update:
				which glide >/dev/null || curl https://glide.sh/get | sh
				which glide-vc >/dev/null || go get -v -u github.com/sgotti/glide-vc
				@echo "removing test files"
				glide vc --only-code --no-tests

tools:
	go get github.com/remyoudompheng/go-misc/deadcode
	go get github.com/alecthomas/gocyclo
	go get github.com/opennota/check/...
	go get github.com/golang/lint/golint
	go get github.com/kisielk/errcheck
	go get -u github.com/kardianos/govendor

.PHONY: default,metalint,deadcode,cyclo,aligncheck,defercheck,structcheck,lint,errcheck,tools
