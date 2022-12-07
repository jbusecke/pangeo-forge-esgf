from pangeo_forge_esgf.utils import ensure_project_str


def test_ensure_project_str():
    # There is a mess of different project_ids in CORDEX ESGF.
    assert ensure_project_str("cordex") == "CORDEX"

    unformatted = ["cordex-reklies", "CORDEX-REKLIES"]
    for p in unformatted:
        assert ensure_project_str(p) == "CORDEX-Reklies"

    unformatted = ["cordex-esd", "CORDEX-esd"]
    for p in unformatted:
        assert ensure_project_str(p) == "CORDEX-ESD"

    unformatted = ["CORDEX-ADJUST", "cordex-adjust"]
    for p in unformatted:
        assert ensure_project_str(p) == "CORDEX-Adjust"
