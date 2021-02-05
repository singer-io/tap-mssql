"""
This module is a template of test objectives to complete
for each stream of a SAAS API
"""

#
# def test_discovery():
#     """
#     WIP
#     # verify that the number of actual streams equals the number of expected streams
#     # verify the actual stream names equal the expected stream names
#     # stream names only have [a-z_]
#     # verify that the annotated schema has the correct properties
#     # verify that the metadata has the correct breadcrumbs / properties --
#     # verify that non pk's and replication fields
#       have inclusion as available in annotated schema.
#     # verify that non pk's and replication fields
#       have inclusion as available in metadata.
#     # verify custom fields are in the schema - TBD
#     # ensure schema doesn't allow extra types? - TBD
#     """


def test_stream_schema():
    """
    TOO MUCH WORK FOR THE VALUE OF THE TEST - MORE APPLICABLE IF THERE IS NO ALPHA TESTER
    PREREQUISITE
    For EACH stream populate the target endpoint with data in each field
    with each expected data type allowed for that field.

    For instance if an optional field can take a float from 0..100
    sample data should include the endpoints of the range, null at a minimum.
    Maybe the data can be stored as a string.
        • 0
        • 0.00001
        • 100
        • 99.9999999
        • "100" (if possible)
        • null value
        • no key (if possible)

    For date-times as an example we should try as many formats as we can to make
    sure we can handle them correctly. Some examples might look like:
        • 2018-04-25T13:51:12-04:00
        • 20080915T155300
        • 20080915T155300Z
        • 2008-09-15
        • null
        • ""

    TEST OBJECTIVES:
        >> • Run a sync with all fields selected and verify there are no errors.
        • verify the sync captured the setup data
        • verify every field is present in the target data for the stream
    """


# def test_stream_pagination():
#     """
#     PREREQUISITE
#     For EACH stream add enough data that you surpass the limit of a single
#     fetch of data.  For instance if you have a limit of 250 records ensure
#     that 251 records have been posted for that stream.
#
#     TEST OBJECTIVES:
#         • Run a sync with ALL fields selected and
#           verify that the number of records sent to the target
#           exceeds the limit
#     """


# def test_only_selected_streams():
#     """
#     Verify that the tap only sends data to the target for selected streams
#
#     Think about child streams.  child without parent and vice versa.
#     Think about this in combination with bookmarks.
#     """


# def test_stream_min_fields():
#     """
#     PREREQUISITE
#     For EACH stream add enough data that you surpass the limit of a single
#     fetch of data.  For instance if you have a limit of 250 records ensure
#     that 251 records have been posted for that stream.
#
#     TEST OBJECTIVES:
#         • Run a sync with NO fields selected (assuming there is at least 1
#           automatic field, or 1 selected if there are no automatic fields)
#           and verify that the number of records sent to the target
#           exceeds the limit
#         • Verify that automatic fields (or 1 selected)
#           are the ONLY ones present in the target data for the stream
#           (plus fields where selected-by-default metadata is set to true)
#     """


def test_field_conflicts():
    """
    PHASE 2

    Test the business rules around field selection.
    Verify that selecting field 1 means you can't select field 2
    """


# def test_stream_inc_bookmarks():
#     """
#     SOME VALUE - NOT SURE IF WE NEED TO TEST BOOKMARK USE. NEED TO ENSURE BOOKMARK SETTING
#     PREREQUISITE
#     For EACH stream that is incrementally replicated there are multiple rows of data.
#
#     NOTE: It is typical that there will be an automatic field that is used as the
#           replication key (bookmark).  If this is not the case, and there are no
#           automatic fields the sync should select a single field that is not
#           the replication key.  (For example S3 uses a replication key that is the
#           file modified date and may not have a primary key, in this case select
#           a single field that isn't relevant to the modified date)
#
#     TEST STEPS
#         • NOTE the start time of the test (for test_start_date below).
#         • Run a sync with NO fields selected for each stream
#           so that the bookmark is up to date
#         • For each stream Update a subset of the records for the stream
#           and note the number n where 0 < n < Total records
#         • For each stream Insert m records 0 < m
#         • NOTE n and m for each stream (for test_start_date below).
#         • Run another sync
#
#     TEST OBJECTIVES:
#         • Verify that the number of records returned in the second sync
#           is equal to n + m + 1 above for each stream. (The tap can find updated
#           records and do not get records that were not updated.)
#     """


# def test_start_date():
#     """
#     PREREQUISITE
#     The test_stream_inc_bookmarks test has been run and no other data has been
#     modified in the dev/test account since that test has been run
#     and you have the start time from that test and the n + m values
#     for each stream for that test.
#
#     TEST STEPS
#         • Run a sync with the start date set to
#           the start time of the test_stream_inc_bookmarks test
#
#     TEST OBJECTIVES:
#         • Verify that the number of records returned in the sync
#           is equal to n + m above for each stream.
#     """

#
# def test_stream_full():
#     """
#     TEST BASED ON NOT SAVING STATE
#     PREREQUISITE
#     For EACH stream that is fully replicated there are multiple rows of data.
#
#     TEST STEPS
#         • Run a sync for each stream
#         • For each stream Update a subset of the records for the stream
#           and note the number n where 0 < n < Total records
#         • For each stream Insert m records 0 < m
#         • Run another sync
#
#     TEST OBJECTIVES:
#         • Verify that the number of records returned in the second sync
#           is equal to the number of records in the first sync + m
#     """


def test_error_handling():
    """TBD"""


def test_performance():
    """
    TBD - This is probably not necessary in tap-tester as it would cause longer
    run times.  This is something that would be useful to do manually.

    Run a large set of data and ensure you don't run into memory leaks, disk
    space issues, runs that take a long time to run.
    """
