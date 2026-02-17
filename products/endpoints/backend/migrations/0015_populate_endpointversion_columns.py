from django.db import migrations


def populate_columns(apps, schema_editor):
    from products.endpoints.backend.models import EndpointVersion as RealEndpointVersion

    EndpointVersion = apps.get_model("endpoints", "EndpointVersion")

    for version in EndpointVersion.objects.select_related("endpoint").filter(columns=[]):
        columns = RealEndpointVersion.extract_columns(version.query, version.endpoint.team_id)
        if columns:
            version.columns = columns
            version.save(update_fields=["columns"])


def reverse_populate_columns(apps, schema_editor):
    EndpointVersion = apps.get_model("endpoints", "EndpointVersion")
    EndpointVersion.objects.update(columns=[])


class Migration(migrations.Migration):
    dependencies = [
        ("endpoints", "0014_endpointversion_columns"),
    ]

    operations = [
        migrations.RunPython(populate_columns, reverse_populate_columns),
    ]
