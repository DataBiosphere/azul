from azul.template import emit

emit({
    "data": [
        {
            "aws_caller_identity": {
                "current": {}
            }
        },
        {
            "aws_region": {
                "current": {}
            }
        },
    ],
    "locals": {
        "account_id": "${data.aws_caller_identity.current.account_id}",
        "region": "${data.aws_region.current.name}"
    }
})
