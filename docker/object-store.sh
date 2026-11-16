#!/bin/sh
set -eu
# Keep this launcher LF-terminated for Docker on Windows.
endpoint=http://object-store:9000
marker=paritylake-object-store-ci-v1

s3() {
    curl --disable --silent --show-error --fail --connect-timeout 5 --max-time 20 \
        --aws-sigv4 "aws:amz:$AWS_REGION:s3" \
        --user "$AWS_ACCESS_KEY_ID:$AWS_SECRET_ACCESS_KEY" "$@"
}

case "${1:-}" in
    init)
        for bucket in warehouse checkpoints; do
            if ! s3 --head --output /dev/null "$endpoint/$bucket" 2>/dev/null; then
                s3 --request PUT --output /dev/null "$endpoint/$bucket"
            fi
            s3 --head --output /dev/null "$endpoint/$bucket"
        done
        ;;
    write)
        s3 --request PUT --data-binary "$marker" --output /dev/null \
            "$endpoint/checkpoints/_ci_object_store_probe"
        ;;
    verify)
        actual="$(s3 "$endpoint/checkpoints/_ci_object_store_probe")"
        if [ "$actual" != "$marker" ]; then
            echo "Object-store probe content mismatch" >&2
            exit 1
        fi
        echo "Object-store probe content verified"
        ;;
    *)
        echo "Usage: object-store.sh {init|write|verify}" >&2
        exit 2
        ;;
esac
