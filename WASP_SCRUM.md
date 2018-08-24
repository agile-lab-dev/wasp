Introduction

In order to focus on getting things done :tm: we propose a LEAN software development methodology based on Scrum. The goal of the proposed methodology is to raise effectiveness and to reduce the communication overhead between the WASP core team and the downstream project owners (stake-holders) by raising their engagement in core development activities and offering a transparent and predictable life-cycle of wasp core releases.

What is Scrum
Source: Scrum (software development))

Scrum is an iterative and incremental framework for managing product development. It defines "a flexible, holistic product development strategy where a development team works as a unit to reach a common goal", challenges assumptions of the "traditional, sequential approach" to product development, and enables teams to self-organize by encouraging physical co-location or close online collaboration of all team members, as well as daily face-to-face communication among all team members and disciplines involved.

A key principle of Scrum is the dual recognition that customers will change their minds about what they want or need (often called requirements volatility) and that there will be unpredictable challenges — for which a predictive or planned approach is not suited. As such, Scrum adopts an evidence-based empirical approach — accepting that the problem cannot be fully understood or defined up front, and instead focusing on how to maximize the team's ability to deliver quickly, to respond to emerging requirements, and to adapt to evolving technologies and changes in market conditions.

The sprint
A sprint (or iteration) is the basic unit of development in Scrum. The sprint is a time-boxed effort; that is, it is restricted to a specific duration. The duration is fixed in advance for each sprint and is normally between one week and one month, with two weeks being the most common.

The product backlog
The product backlog comprises an ordered list of requirements that a scrum team maintains for a product. It consists of features, bug fixes, non-functional requirements, etc. — whatever must be done to successfully deliver a viable product. The product owner prioritizes those product backlog items (PBIs) based on considerations such as risk, business value, dependencies, size, and date needed.

The Sprint Planning
At the beginning of a sprint, the scrum team holds a sprint planning event to:
Mutually discuss and agree on the scope of work that is intended to be done during that sprint
Select product backlog items that can be completed in one sprint
Prepare a sprint backlog that includes the work needed to complete the selected product backlog items

The Daily scrum (Stand-up meeting)
Each day during a sprint, the team holds a daily scrum (or stand-up) with specific guidelines:
Boxed duration of 15minutes
All members of the development team come prepared.
Anyone is welcome, though only development team members should contribute.
Each team member answers: What did I complete yesterday that contributed to the team meeting our sprint goal?
Each team member answers: What do I plan to complete today to contribute to the team meeting our sprint goal?
Each team member answers: Do I see any impediment that could prevent me or the team from meeting our sprint goal?

Any impediment (e.g., stumbling block, risk, issue, delayed dependency, assumption proved unfounded) identified in the daily scrum should be captured by the scrum master.

The Sprint review
At the sprint review, the team:
Reviews the work that was completed and the planned work that was not completed
Presents the completed work to the stakeholders (a.k.a. the demo)
The team and the stakeholders collaborate on what to work on next

The Sprint retrospective
At the sprint retrospective, the team:
Reflects on the past sprint
Identifies and agrees on continuous process improvement actions

Scrum customized for WASP development
Teams approaching Scrum should understand that Scrum is a framework and that should be customized to the specific use case to be effective.
Trying to implement everything in one go and as "prescribed" by the Scrum official guidelines is an often made mistake. 

Here we propose a Scrum customization to WASP development.

WASP Scrum roles

Product owner
Nicolo' Bidotti

The product owner has short to long term visibility on what should be done, how should it be done (acceptance of developed feature) and when (priority).

The product owner is involved in Sprint Planning by estimating each Backlog item and by defining the content of each sprint taking in account the development team's and the stake holder's feedback.

The product owner is aware of the capacity of the team and knows the domain of the project, he is able of balancing development and stake holders contributions.

The product owner is also involved during the Sprint Review, he attends the demo and confirms that the sprint scope has been met (or not).

Scrum master
Andrea Fonti

The Scrum Master responsibility is:
Facilitate the scrum adoption by the team
Schedule meetings
Assemble meeting deliverables (e.g. minutes)
Help the team adopt the continuous improvement mindset

Scrum developer
Andrea Fonti
Davide Colombatto
The scrum developer participates in Sprint Planning, Daily
Scrum, Reviews and Retrospectives

Each developer is responsible of the Sprint Goal, the team wins as a whole and fails as a whole.

A developer is also responsible of the continuous improvement of the Scrum process

Stake holders

- **Product Owner**: Paolo Platter
- **Users**: AgileLab internal projects
- **Maintainer:**
  - Andrea Fonti
  - Nicolò Bidotti
  - Antonio Murgia

Stake holders are subjects that need things-done-as-quickly-as-possible :tm: and as close as possible to their need:
They can influence the planning of a sprint by agreeing with the product owner
They monitor and are aware of implemented features by attending sprint review meeting (in person, online, watching a screencast, reading the minutes) so they take the responsibility to be ambassador of WASP core development to their project.

Wasp Scrum Events

Sprint
A wasp sprint is 1 week long ad corresponds to the release of a minor version at the end of the week, the scope of the sprint is defined at the planning meeting.

Hotfixes
Hotfixes are not comprised in the normal sprint scope (an hotfix should be an anomaly instead of the norm)

Planning
Monday 1 hour - Attended by: Developers, Product Owner, Scrum Master, Stake holders

The scope of the sprint for the week is defined at the planning meeting, each attendant can contribute to its definition and is able to steer the sprint goal.

Review
Friday 1 hour - Attended by: Developers, Product Owner, Scrum Master, Stake holders

The team presents the completed work, a demo is recommended (raises engagement and is an opportunity to discuss improvement or to refine scope of next sprint)

The demo will be performed via screencast tool to serve as reference if any member of the team or of the stake holders should find it useful

Retrospective
Friday 1 hour - Attended by: Developers, Scrum Master, Product Owner

The retrospective is a moment to discuss what should change in the process itself to serve a better purpose in getting-things-done:tm:


Wasp Definition of done
The definition of done (DoD) should be agreed by all the scrum team in order to allow a quantitative (or qualitative if using labels ) approach in the identification of what should be done differently in order to converge to expected quality standards by the stakeholders

Examples of definition of done
Coded
Coded - Has unit test
Coded - Has Unit Test - Has scaladoc
Coded - Has Unit Test - Has scaladoc - Has Integration Tests
Coded - Has Unit Test - Has scaladoc - Has user documentation