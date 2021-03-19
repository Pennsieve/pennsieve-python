Working with the Knowledge Graph
================================

This tutorial provides a set of examples that show how to interact with
the `Pennsieve Knowledge
graph <http://help.pennsieve.com/pennsieve-web-application/pennsieve-knowledge-graph/overview-of-the-pennsieve-knowledge-graph>`__.
Some of the topics covered here include:

-  creating new models
-  creating new records
-  adding relationships between the created records

Setup
-----

Let’s start by creating a dataset that we can use for this tutorial. If
you would like to find out more information about moving, uploading,
downloading data or other data catalog operations, please see
:ref:`tutorials/data_catalog:Working with the Data Catalog`.

Connect to Pennsieve
~~~~~~~~~~~~~~~~~~~~

.. code:: python

    from pennsieve import Pennsieve

    # create a client instance
    ps = Pennsieve()

Tutorial Dataset
~~~~~~~~~~~~~~~~

Knowledge graphs belong to a dataset. If you do not have an existing
dataset to use, you can create one for this tutorial:

.. code:: python

    ds = ps.create_dataset("Mark's Knowledge Graph Tutorial")

If you have an existing dataset to use for the tutorial, you can use
``ps.get_dataset(...)`` to retrieve before continuing.

Defining and Creating Models
----------------------------

Here, we will create a few models using the client.

Models to Create
~~~~~~~~~~~~~~~~

The following represents our intended set of knowledge graph models,
with their corresponding properties.


| Participant
|   - subject_id
|   - name
|   - age
|
| Visit
|   - visit_id
|   - date
|   - reason
|
| EEG
|   - room_number
|   - machine_model
|   - administrator
|
| Exam
|   - context
|   - mood
|   - motor_skill
|


Define Model Schemas
~~~~~~~~~~~~~~~~~~~~

In the following section, we will define each model schema and then use these to create each model on the platform. There are several approaches to creating models, however, for simplicity we will only showcase a single method which uses ``ModelProperty`` objects directly.

**Allowable data types**

The following values can be used to specify the data type. In Pennsieve,
there is no distinction between the different values for each data type, e.g. ``long`` and ``int`` will result in the same Pennsieve data type (``Integer``).

:String: ``"string"``, ``str``, or ``unicode`` (if python2)
:Integer: ``"long"``, ``int``, ``long`` (if python2)
:Decimal: ``"double"`` or ``float``
:Boolean: ``"boolean"`` or ``bool``
:Date: ``"date"`` or ``datetime.datetime``

.. note:: The default data type for a property is a **String**, unless specified otherwise.

.. warning:: Currently, you must specify at least one Model property to serve as a "title". Ensure you set the ``title=True`` for at least one property in your Model's schema, otherwise you may encounter an error.


Participant
^^^^^^^^^^^

.. code:: python

    # we will use ModelProperty to define our schemas
    from pennsieve import ModelProperty

    participant_schema = [
        ModelProperty('name', title=True),
        ModelProperty('subject_id', data_type=int),
        ModelProperty('age',  data_type=int)
    ]

Visit
^^^^^

.. code:: python

    visit_schema = [
        ModelProperty('visit_id', title=True),
        ModelProperty('date', data_type='date'),
        ModelProperty('reason')
    ]

EEG
^^^

.. code:: python

    eeg_schema = [
        ModelProperty('room_number', title=True),
        ModelProperty('machine_model'),
        ModelProperty('administrator')
    ]

Exam
^^^^

.. code:: python

    exam_schema = [
        ModelProperty('context', title=True),
        ModelProperty('mood', data_type=int),
        ModelProperty('motor_skill', data_type=float),
    ]

Create Models
~~~~~~~~~~~~~

Knowledge graphs, and the models within them, are tied to a dataset.
Using the defined schemas, we can now create models on the Pennsieve
platform within the tutorial dataset. Once we execute
``create_model(...)`` the model will appear on the dataset’s knowledge
graph section.

.. code:: python

    ds.create_model('Participant', schema = participant_schema)
    ds.create_model('Visit',       schema = visit_schema)
    ds.create_model('EEG',         schema = eeg_schema)
    ds.create_model('Exam',        schema = exam_schema)

Creating Records
----------------

In this section we will create model instances, referred to as
“records”, for each of the models that we have created so far.

Creating an individual record
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Here we will create one record for the ``Participant`` model.

.. code:: python

    # get the the model
    participant = ds.get_model('Participant')

    # create a new participant in the graph
    pt_123 = participant.create_record({
        'name': 'Karl',
        'age': 34,
        'subject_id': 123
    })

Congratulations, you just created your first Participant record! The
variable ``pt_123`` (of type ``Record``) can now be used to manipulate
the record values and/or relate to other records.

Creating multiple records
~~~~~~~~~~~~~~~~~~~~~~~~~

We can also create multiple records at the same time through the
``create_records()`` method.

.. code:: python

    participant_values = [
        {'name': 'Lucy',   'age': 67, 'subject_id': 200},
        {'name': 'Silvia', 'age': 70, 'subject_id': 300},
        {'name': 'Zach',   'age': 55, 'subject_id': 400},
    ]

    participant.create_records(participant_values)


Retrieving Records
------------------

You can easily retrieve your created records using the ``get_all()`` method.

.. code:: python

    all_pts = participant.get_all()

And easily transform the result into a Panda's ``DataFrame`` object:

.. code:: python

    all_pts.as_dataframe()

.. raw:: html

    <div>
    <style scoped>
        .dataframe tbody tr th:only-of-type {
            vertical-align: middle;
        }

        .dataframe tbody tr th {
            vertical-align: top;
        }

        .dataframe thead th {
            text-align: right;
        }
    </style>
    <table border="1" class="dataframe">
      <thead>
        <tr style="text-align: right;">
          <th></th>
          <th>age</th>
          <th>subject_id</th>
          <th>name</th>
        </tr>
      </thead>
      <tbody>
        <tr>
          <th>0</th>
          <td>34</td>
          <td>123</td>
          <td>Karl</td>
        </tr>
        <tr>
          <th>1</th>
          <td>67</td>
          <td>200</td>
          <td>Lucy</td>
        </tr>
        <tr>
          <th>2</th>
          <td>70</td>
          <td>300</td>
          <td>Silvia</td>
        </tr>
        <tr>
          <th>3</th>
          <td>55</td>
          <td>400</td>
          <td>Zach</td>
        </tr>
      </tbody>
    </table>
    </div>
    <br />


Relating Records
----------------

Basics (example)
~~~~~~~~~~~~~~~~

Relating records is done via ``some_record.relate_to(...)`` method, which will relate ``some_record`` to a single record, a list of records, or a data package.

The follow examples showcase this method, but will not work unless ``visit_1``, ``visit_2``, etc. exist.

.. code:: python

    pt_123.relate_to(visit_1)

will relate record ``pt_123`` with record ``visit1``. You can relate many records by supplying a list of records:

.. code:: python

    pt_123.relate_to([visit1, visit2, visit3])


Relating Participant to Visit Records
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Utilizing the methods above, we will create a series of Visits for each Participant and relate them to the Participant.

.. code:: python

    from datetime import datetime

    visit = ds.get_model('Visit')

    for pt in participant.get_all():

        # create 4 fake visits per participant
        pt_visits = visit.create_records([
            {'visit_id': 1, 'date': datetime(2018,12,1), 'reason': 'screening'},
            {'visit_id': 2, 'date': datetime(2018,12,2), 'reason': 'visit 1'},
            {'visit_id': 3, 'date': datetime(2018,12,3), 'reason': 'visit 2'},
            {'visit_id': 4, 'date': datetime(2018,12,4), 'reason': 'final visit'},
        ])

        # and link the visits to the participant (pt)
        pt.relate_to(pt_visits)


Relating Visits to EEG and Exam Records
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Similarly, for each Visit we will create an EEG record and two Exam records (before and after the EEG). Additionally, we will utilize the ``relationship_type`` argument to set the relationship type between Visit and EEGs/Exams as "collected", i.e. ``visit_1 --collected--> exam_1``.

.. code:: python

    from random import randint, random

    eeg = ds.get_model('EEG')
    exam = ds.get_model('Exam')

    for a_visit in visit.get_all():

        # One EEG per visit
        visit_eeg = eeg.create_record({
            'room_number': 4128,
            'machine_model': 'Starstim R32',
            'administrator': 'Kevin'
        })

        # relate to visit
        a_visit.relate_to(visit_eeg, relationship_type='collected')

        # Two exams per visit (before/after EEG)
        visit_exam1 = exam.create_record({
            'context': 'before',
            'mood': randint(1,10),
            'motor_skill': round(random()*10, 2)
        })
        visit_exam2 = exam.create_record({
            'context': 'after',
            'mood': randint(1,10),
            'motor_skill': round(random()*10, 2)
        })

        # relate exams to visit
        a_visit.relate_to([visit_exam1, visit_exam2], relationship_type='collected')


Relating Records to Files
~~~~~~~~~~~~~~~~~~~~~~~~~

Records can be related to files on the Pennsieve Platform. Files are represented as ``DataPackage`` objects in the data catalog.

For example, in our current graph, we would likely want each EEG record to relate to a ``DataPackage`` that is an uploaded EEG file representing the EEG session. Let's assume that there are files named ``EEG 1``, ``EEG 2``, etc. in our current dataset, you would go about linking these files just as if they are other records:

.. code:: python

    eeg = ds.get_model('EEG')

    for i, eeg_record in enumerate(eeg.get_all()):
        # get the corresponding EEG file
        eeg_file = ds.get_items_by_name('EEG ' + i)[0]

        # relate to the current record
        eeg_record.relate_to(eeg_file)


Congratulations — you have successfully created a knowledge graph on the Pennsieve platform!
