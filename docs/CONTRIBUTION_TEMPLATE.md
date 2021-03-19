# Introduction

Thank you for considering contributing to the Pennsieve Python client. It's people like you that make the client such a great tool.

Following these guidelines helps to communicate that you respect the time of the developers managing and developing this open source project. In return, they should reciprocate that respect in addressing your issue, assessing changes, and helping you finalize your pull requests.

Keep an open mind! Improving documentation, bug triaging, or writing tutorials are all examples of helpful contributions that mean less work for you.

Please do not use the issue tracker for support questions. Please refer to the `Help & Info` section within the application to interact directly with someone from Pennsieve for support.

# Expectations
This includes not just how to communicate with others (being respectful, considerate, etc) but also technical responsibilities (importance of testing, project dependencies, etc).

* Ensure cross-platform compatibility for every change that's accepted. Windows, Mac, Debian & Ubuntu Linux.
* Ensure compatibility with all supported Python versions. See [Supporting Python 2 and 3](#supporting-python-2-and-3) for more information.
* Create issues for any major changes and enhancements that you wish to make. Discuss things transparently and get community feedback.
* Don't add any classes to the codebase unless absolutely needed. Err on the side of using functions.
* Keep feature versions as small as possible, preferably one new feature per version.
* Be welcoming to newcomers and encourage diverse new contributors from all backgrounds. See the [Python Community Code of Conduct](https://www.python.org/psf/codeofconduct/).


# Your First Contribution
Unsure where to begin contributing? There are tickets labeled `good first issue` to help you get familiar with the codebase while helping us to improve the client.

At this point, you're ready to make your changes! Feel free to ask for help; everyone is a beginner at first :smile_cat:

# Getting started
1. Create an issue
2. Create your own fork of the code
2. Do the changes in your fork
3. Submit your pull request

# Code review process
The core team looks at Pull Requests on a regular basis in a weekly triage meeting. After feedback has been given we expect responses within the month. After one month, we may close the pull request if it isn't showing any activity.

# Supporting Python 2 and 3
We currently support Python 2.7 and 3.4-3.7. In general, we try and write Python 3 code that will run on Python 2.7 using backports provided by [future](http://python-future.org). Keep the following in mind as you code:

* All modules must include the following import statement:

        from __future__ import absolute_import, division, print_function

* Import the following from `builtins` as needed:

   * `map`, `zip`, `range`, `filter` (return Python 3 style iterators instead of lists)
   * `dict` (`.keys()`, `.values()`, and `.items()` all return iterators)
   * `object` (new-style classes)

   See [python-future.org](http://python-future.org) for a complete list of other builtins backported from Python 3.

* When performing type checks, use the following conventions:

        from future.utils import string_types, integer_types
        if isinstance(x, string_types):
            print('x is a string')
        elif isinstance(x, integer_types):
            print('x is an integer')

* Explicitly mark all unicode strings with `u"..."` prefixes.

Note that you should *not* import anything from `builtins` or `__future__` in test modules. Test code should emulate in-the-wild usage as much as possible.

If you use PyCharm you can enable code inspections to automatically check compatibility with all supported Python versions.
