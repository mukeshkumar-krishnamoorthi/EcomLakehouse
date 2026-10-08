from ecomlakehouse.utils.storage import (
    get_bucket_name,
    get_s3_client,
)


def main():
    s3 = get_s3_client()
    bucket = get_bucket_name()

    response = s3.list_buckets()

    print("Available buckets:")

    for item in response["Buckets"]:
        print(f"- {item['Name']}")

    print(f"\nTarget bucket: {bucket}")


if __name__ == "__main__":
    main()
