#!/usr/bin/python

import logging
import json
import datetime
import uuid
import time
from sqlalchemy.exc import SQLAlchemyError, NoSuchColumnError, IntegrityError
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.sql import and_
from sqlalchemy.orm import aliased
from sqlalchemy.sql import func

import linqia.system
from linqia.utilities.dictobj import as_dict
from linqia.utilities.session_scope import session_scope
from linqia.bottle.jsonutil import getJsonParams
from linqia.utilities.column_helper import ColumnHelper
from linqia.utilities.crud import ReadObject, CrudObject, CrudResponseException, CrudResponseJson, CrudResponsePlain
from linqia.utilities.proxy import REEF_USER_ID
from linqia.utilities.guest_token import create_token, update_token, get_tokens, delete_token
from linqia.utilities.caching2 import LinqiaRedisSession
from linqia.models.campaign import Campaign
from linqia.models.customer import CustomerCampaignSet, CustomerBrand
from linqia.models.user import Users, Notification, UserHasNotification
from linqia.models.workflow import ProgramWorkflow, ProgramWorkflowGroup, ProgramWorkflowMember, \
                                   ProgramWorkflowApprovalMember, ProgramWorkflowApproval, ProgramWorkflowGroup, \
                                   ProgramWorkflowApprovalComment, ProgramInfluencerPostMedia, ProgramInfluencerMedia, \
                                   ProgramInfluencerPost, ProgramWorkflowApprovalVersion, ProgramWorkflowApprovalCommentReview, \
                                   GuestToken, ProgramWorkflowStep
from linqia.models.list import CustomerListInfluencerHistory, CustomerListInfluencer, CustomerList, CustomerListHistory
from linqia.models.influencer import Influencer
from linqia.models.user import Users, UserPicture, UserGender, UserRace, Countries, States
from notification.utils import send_notification

import linqia.utilities.actions as PA

from linqia.utilities.linqia_logging import logger

from workflow_perms import update_permissions_member, update_permissions_approval

# Statuses
from constants import APPROVED, AWAITING_INFLUENCER_REDO, AWAITING_INFLUENCER_REVIEW, AWAITING_RESPONSE, \
                      AWAITING_REVIEW, CONFIRMED, DONE, FINALIZED, IGNORED, NEEDS_EDITS, NEEDS_REDO, NEW, \
                      NOT_YET_RECEIVED, PENDING, READY_TO_PUBLISH, READY_TO_SEND, REJECTED, REQUEST_REDO, \
                      REVIEWED, SKIPPED, SUBMITTED, SENDER_USER_ID, SENDER_GUEST_TOKEN

# Member Types
from constants import REVIEWER, APPROVER, EMPLOYEE, INFLUENCER, GUEST

# Workflow Types
from constants import RESONATE_BRIEF, RESONATE_CONTENT, RESONATE_INFLUENCER_LIST

def workflow_share(db_session, program_id, workflow_id, member_id, user_id, email):
    if user_id is not None and email is not None:
        raise CrudResponsePlain("Set user_id or email but not both", 400)
    if user_id is None and email is None:
        raise CrudResponsePlain("Set user_id or email but not both", 400)
    # The calling member must be active in at least one step
    member = db_session.query(ProgramWorkflowMember) \
                       .filter(ProgramWorkflowMember.id == member_id) \
                       .filter(ProgramWorkflowMember.program_workflow_id == workflow_id) \
                       .filter(ProgramWorkflow.program_id == program_id) \
                       .filter(ProgramWorkflow.id == ProgramWorkflowMember.program_workflow_id).first()
    if member is None:
        raise CrudResponsePlain("Member not found", 404)
    if member.member_type not in (EMPLOYEE, APPROVER):
        raise CrudResponsePlain("Member must be an employee or approver", 400)
    new_member = ProgramWorkflowMember()
    new_member.program_workflow_id = member.program_workflow_id
    new_member.step_id = member.step_id
    new_member.member_type = REVIEWER
    new_member.user_id = user_id
    new_member.email = email
    new_member.created_at = datetime.datetime.utcnow()
    new_member.updated_at = datetime.datetime.utcnow()
    db_session.add(new_member)
    db_session.commit()
    workflow_update_new_member(db_session, new_member.id)
    # fire off notification for reviewer receiving influencers if approvals exist
    approvals = db_session.query(ProgramWorkflowApproval) \
        .filter(ProgramWorkflowApproval.program_workflow_id == ProgramWorkflowMember.program_workflow_id) \
        .filter(ProgramWorkflowMember.id == new_member.id) \
        .all()
    if len(approvals):
        send_infl_review_notification(db_session, new_member, 'infl_review_reviewer_receives_infls')
    return new_member.id

def workflow_update_new_member(db_session, member_id):
    member, sequence = db_session.query(ProgramWorkflowMember, ProgramWorkflowStep.sequence) \
        .filter(ProgramWorkflowMember.id == member_id) \
        .filter(ProgramWorkflowStep.id == ProgramWorkflowMember.step_id).one()
    if member is None:
        raise CrudResponsePlain("Member not found", 404)

    approvals = []
    query = db_session.query(ProgramWorkflowApprovalMember.approval_id, ProgramWorkflowMember.step_id) \
        .filter(ProgramWorkflowMember.program_workflow_id == member.program_workflow_id) \
        .filter(ProgramWorkflowApprovalMember.workflow_member_id == ProgramWorkflowMember.id) \
        .filter(ProgramWorkflowApprovalMember.status == AWAITING_RESPONSE) \
        .filter(ProgramWorkflowMember.step_id == ProgramWorkflowStep.id) \
        .filter(ProgramWorkflowStep.sequence >= sequence).distinct()

    for approval_id, step_id in query:
        am = ProgramWorkflowApprovalMember()
        am.approval_id = approval_id
        am.workflow_member_id = member.id
        if step_id == member.step_id:
            am.status = AWAITING_RESPONSE
        else:
            am.status = SKIPPED
        am.created_at = datetime.datetime.utcnow()
        am.updated_at = datetime.datetime.utcnow()
        db_session.add(am)
        approvals.append(am)

    # For any approvals that have been approved or rejected the member gets to see it.
    query = db_session.query(ProgramWorkflowApproval.id, ProgramWorkflowApproval.status) \
        .filter(ProgramWorkflowMember.id == member_id) \
        .filter(ProgramWorkflowMember.program_workflow_id == ProgramWorkflowApproval.program_workflow_id) \
        .filter(ProgramWorkflowApproval.status.in_([APPROVED, REJECTED]))

    for r in query:
        q = db_session.query(ProgramWorkflowApprovalMember) \
            .filter(ProgramWorkflowApprovalMember.workflow_member_id == member_id) \
            .filter(ProgramWorkflowApprovalMember.approval_id == r.id).first()
        if q is None:
            am = ProgramWorkflowApprovalMember()
            am.approval_id = r.id
            am.workflow_member_id = member_id
            am.status = SKIPPED
            am.created_at = datetime.datetime.utcnow()
            am.updated_at = datetime.datetime.utcnow()
            db_session.add(am)
            approvals.append(am)

    db_session.commit()
    for am in approvals:
        workflow_user_activated(db_session, am.id)
    update_permissions_member(db_session, member_id)


def new_approval_member(approval_id, member_id):
    member = ProgramWorkflowApprovalMember()
    member.approval_id = approval_id
    member.workflow_member_id = member_id
    member.status = AWAITING_RESPONSE
    member.created_at = datetime.datetime.utcnow()
    return member

def workflow_user_activated(db_session, approval_member_id):
    pass

def workflow_user_deactivated(db_session, approval_member_id):
    pass

# Mark anyone who has not done there review as skipped
def workflow_approval_close(db_session, approval_id):
    q = db_session.query(ProgramWorkflowApprovalMember) \
        .filter(ProgramWorkflowApprovalMember.approval_id == approval_id) \
        .filter(ProgramWorkflowApprovalMember.status == AWAITING_RESPONSE)
    updated = []
    for am in q:
        am.status == SKIPPED
        updated.append(am)
    db_session.commit()
    for am in updated:
        workflow_user_deactivated(db_session, am.id)
        
def update_workflow_finished(db_session, approval_id):
    pwa, pw = db_session.query(ProgramWorkflowApproval, ProgramWorkflow) \
        .filter(ProgramWorkflowApproval.id == approval_id) \
        .filter(ProgramWorkflow.id == ProgramWorkflowApproval.program_workflow_id).one()

    if pwa.status not in (APPROVED, REJECTED):
        pwa.status = APPROVED
        db_session.add(pwa)
        db_session.commit()
    workflow_approval_close(db_session, approval_id)

    if pw.object_type == RESONATE_INFLUENCER_LIST:
        pass
    else:
        raise Exception("Finish workflow not yet implemented for type {}".format(pw.object_type))

def update_workflow_rejected(db_session, approval_id):
    pwa, pw = db_session.query(ProgramWorkflowApproval, ProgramWorkflow) \
        .filter(ProgramWorkflowApproval.id == approval_id) \
        .filter(ProgramWorkflow.id == ProgramWorkflowApproval.program_workflow_id).one()
    pwa.status = REJECTED
    db_session.add(pwa)
    db_session.commit()
    workflow_approval_close(db_session, approval_id)

def workflow_user_update(db_session, approval_member_id):
    am = db_session.query(ProgramWorkflowApprovalMember).filter(ProgramWorkflowApprovalMember.id == approval_member_id).one()
    members = [ approval_member_id ]
    if am.status != AWAITING_RESPONSE:
        wm, sequence = db_session.query(ProgramWorkflowMember, ProgramWorkflowStep.sequence) \
            .filter(ProgramWorkflowMember.id == am.workflow_member_id) \
            .filter(ProgramWorkflowStep.id == ProgramWorkflowMember.step_id).one()
        if wm.member_type in (APPROVER, EMPLOYEE):
            q = db_session.query(ProgramWorkflowApprovalMember) \
                .filter(ProgramWorkflowApprovalMember.approval_id == am.approval_id) \
                .filter(ProgramWorkflowApprovalMember.workflow_member_id == ProgramWorkflowMember.id) \
                .filter(ProgramWorkflowMember.step_id == wm.step_id) \
                .filter(ProgramWorkflowApprovalMember.id != am.id)
            for other in q:
                if other.status == AWAITING_RESPONSE:
                    other.status = SKIPPED
                members.append(other.id)
            db_session.commit()
            if am.status == REJECTED:
                update_workflow_rejected(db_session, am.approval_id)
            else:
                still_active = workflow_start_flow_step(db_session, wm.program_workflow_id, am.approval_id, sequence, False)
                for amid in members:
                    workflow_user_deactivated(db_session, amid)

                if not still_active:
                    update_workflow_finished(db_session, am.approval_id)
    update_permissions_member(db_session, am.workflow_member_id)


def workflow_start_flow_step(db_session, workflow_id, approval_id, last_sequence, single_transaction):
    logger.info("{} {} {} {}".format(workflow_id, approval_id, last_sequence, single_transaction))
    query = db_session.query(ProgramWorkflowStep) \
                     .filter(ProgramWorkflowStep.program_workflow_id == workflow_id) \
                     .filter(ProgramWorkflowStep.id == ProgramWorkflowMember.step_id) \
                     .order_by(ProgramWorkflowStep.sequence)
    if last_sequence is not None:
        query = query.filter(ProgramWorkflowStep.sequence > last_sequence)
    step = query.first()
    members = []
    if step is not None:
        query = db_session.query(ProgramWorkflowMember).filter(ProgramWorkflowMember.step_id == step.id)
        for pwm in query:
            member = new_approval_member(approval_id, pwm.id)
            db_session.add(member)
            members.append(member)
        if single_transaction:
            db_session.flush()
            update_permissions_approval(db_session, approval_id, True)
        else:
            db_session.commit()
            update_permissions_approval(db_session, approval_id)
        for m in members:
            workflow_user_activated(db_session, m.id)
    return len(members)


def find_name_or_email_from_user_id(db_session, pwm):
    if pwm:
        if isinstance(pwm, ProgramWorkflowMember):
            if pwm.user_id:
                user_record = db_session.query(Users).get(pwm.user_id)
                _name = ' '.join(filter(None, [user_record.name_first, user_record.name_last]))
                return _name, user_record.email
            return None, pwm.email
        elif isinstance(pwm, GuestToken):
            return pwm.name, pwm.email
        else:
            name = None
            email = None
            if hasattr(pwm, 'name'):
                name = getattr(pwm, 'name')
            if hasattr(pwm, 'email'):
                email = getattr(pwm, 'email')
            return name, email
    return None, None

def workflow_send(db_session, program_id, wf_id, wfa_ids, sharer_wfm_id, reviewer_note, sender_user_id=None,
               sender_guest_token=None):
    try:
        program_id = long(program_id)
        sharer = db_session.query(ProgramWorkflowMember) \
            .filter(ProgramWorkflowMember.id == sharer_wfm_id).first()
        if sharer is None:
            raise CrudResponsePlain("program_workflow_member {} is not found".format(sharer_wfm_id), 404)

        # notifications_final = []
        # sharer_name, sharer_email = find_name_or_email_from_user_id(db_session, sharer)
        # sharer_name = sharer_email if not sharer_name else sharer_name
        # member_type = sharer.member_type
        wpam_objects = []
        for wfa_id in [int(z) for z in wfa_ids]:
            try:
                pwa, pwam = db_session.query(ProgramWorkflowApproval, ProgramWorkflowApprovalMember) \
                    .filter(ProgramWorkflowApproval.id == wfa_id) \
                    .filter(ProgramWorkflowApproval.id == ProgramWorkflowApprovalMember.approval_id) \
                    .filter(ProgramWorkflowApprovalMember.workflow_member_id == sharer.id).one()
                if pwam.status != AWAITING_RESPONSE:
                    raise CrudResponsePlain("approval {} is not AWAITING_RESPONSE".format(wfa_id), 400)
                if pwam.pending_status == None or pwam.pending_status == pwam.status:
                    raise CrudResponsePlain("approval {} pending status is not set".format(wfa_id), 400)
                wpam_objects.append(pwam)
            except NoResultFound as e:
                raise CrudResponsePlain("aproval {} is not found".format(wfa_id), 404)

        # Validiated, now we can do it.
        for pwam in wpam_objects:
            pwam.status = pwam.pending_status
            db_session.add(pwam)
        db_session.commit()

        for pwam in wpam_objects:
            workflow_user_update(db_session, pwam.id)

        employees = get_workflow_employees(db_session, wf_id)
        if sharer.member_type == APPROVER:
            for employee in employees:
                send_infl_review_notification(db_session, employee, 'infl_review_approver_submits_infls')

        elif sharer.member_type == REVIEWER:
            for employee in employees:
                send_infl_review_notification(db_session, employee, 'infl_review_reviewer_submits_infls', sharer)

            approver= db_session.query(ProgramWorkflowMember) \
                .filter(ProgramWorkflowMember.member_type == APPROVER) \
                .filter(ProgramWorkflowMember.program_workflow_id == wf_id) \
                .filter(ProgramWorkflowMember.is_deleted == False) \
                .first()

            if approver is not None:
                send_infl_review_notification(db_session, approver, 'infl_review_reviewer_submits_infls', sharer)

    except CrudResponsePlain as e:
        logger.exception(e)
        raise e

    pass

def get_workflow_employees(db_session, workflow_id):
    return db_session.query(ProgramWorkflowMember) \
        .filter(ProgramWorkflowMember.member_type == EMPLOYEE) \
        .filter(ProgramWorkflowMember.is_deleted == False) \
        .filter(ProgramWorkflowMember.program_workflow_id == workflow_id) \
        .all()

def get_object_values(db_session, workfow_id, object_id):
    pw = db_session.query(ProgramWorkflow) \
                   .filter(ProgramWorkflow.id == workfow_id).one()

    if pw.object_type == RESONATE_INFLUENCER_LIST:
        data = db_session.query(CustomerListInfluencer, UserGender.description, UserRace.name, Countries.name, States.name) \
                             .filter(CustomerListInfluencer.id == object_id) \
                             .outerjoin(UserGender, UserGender.name == CustomerListInfluencer.gender) \
                             .outerjoin(UserRace, UserRace.id == CustomerListInfluencer.ethnicity_id) \
                             .outerjoin(Countries, Countries.id == CustomerListInfluencer.country_id) \
                             .outerjoin(States, States.id == CustomerListInfluencer.state_id) \
                             .first()
        if data is not None:
            list_inf, gender, race, country, state = data
            val = {
                "influencer_guid": list_inf.influencer_guid,
                "status": list_inf.status,
                "gender": gender,
                "ethnicity": race,
                "city": list_inf.city,
                "state": state,
                "country": country,
                "rationale": list_inf.rationale
            }
            return val
    elif pw.object_type == RESONATE_BRIEF:
        return {"not_implemented": pw.object_type}
    elif pw.object_type == RESONATE_CONTENT:
        return {"not_implemented": pw.object_type}
    else:
        return {"not_defined": True}
    return as_dict(pw)

def send_infl_review_notification(db_session, workflow_member, message_type, reviewer=None):
    # optional reviewer param is for reviewer_submits_influencers notification only to denote the reviewer name
    # in the email body
    campaign_query = db_session.query(CustomerCampaignSet.id, CustomerCampaignSet.name) \
        .filter(CustomerCampaignSet.id == Campaign.campaign_set_id) \
        .filter(Campaign.id == ProgramWorkflow.program_id) \
        .filter(ProgramWorkflow.id == ProgramWorkflowMember.program_workflow_id) \
        .filter(ProgramWorkflowMember.id == workflow_member.id) \
        .first()

    mk_count = db_session.query(func.count(Notification.message_key)) \
        .filter(Notification.message_type == message_type) \
        .filter(Notification.message_key.like('workflow_member_{}%'.format(workflow_member.id))) \
        .first()
    message_key = 'workflow_member_{}_{}'.format(workflow_member.id, mk_count[0] + 1)

    if message_type == 'infl_review_approver_submits_infls':
        influencer_review_link = linqia.system.config.notification.influencer_review_link.format(
            linqia.system.config.hosts.saas, int(campaign_query.id), 'approved',
        )
    else:
        influencer_review_link = linqia.system.config.notification.influencer_review_link.format(
            linqia.system.config.hosts.saas, int(campaign_query.id), 'needs_review',
        )

    if workflow_member.email is not None:
        hashcode_query = db_session.query(GuestToken.hashcode) \
            .filter(GuestToken.id == ProgramWorkflowApprovalMember.token_id) \
            .filter(ProgramWorkflowApprovalMember.workflow_member_id == workflow_member.id) \
            .first()
        if hashcode_query is not None:
            influencer_review_link = '{}&guest_token={}'.format(influencer_review_link, hashcode_query.hashcode)
        else:
            raise CrudResponsePlain("Error while sending '{}' notification for Workflow Member {}: guest token hashcode not found"
                                    .format(message_type, workflow_member.id), status_code=400)


    variables = {
        "campaign_name": campaign_query.name,
        "influencer_review_link": influencer_review_link,
    }

    greet_text, to_email = "", None
    # if recipient is a guest, just set the greeting text to their email
    if workflow_member.user_id is not None:
        name_or_email_query = db_session.query(Users.name_first, Users.name_last, Users.email) \
            .filter(Users.id == workflow_member.user_id) \
            .first()

        if name_or_email_query is not None:
            name_first, name_last, email = name_or_email_query.name_first, name_or_email_query.name_last, \
                                           name_or_email_query.email
            greet_text = '{} {}'.format(name_first, name_last) if name_first and name_last else email
    elif workflow_member.email is not None:
        greet_text, to_email = workflow_member.email, workflow_member.email

    if message_type in ['infl_review_approver_receives_infls', 'infl_review_reviewer_submits_infls']:
        variables["approver_name_full_or_email"] = greet_text
    elif message_type == 'infl_review_reviewer_receives_infls':
        variables["reviewer_name_full_or_email"] = greet_text
    if message_type == 'infl_review_approver_submits_infls':
        variables['bsm_name'] = greet_text

    if message_type == 'infl_review_reviewer_receives_infls' or message_type == 'infl_review_approver_submits_infls':
        approver_name = ""
        # if approver is guest then just set approver_name to email
        approver_query = db_session.query(ProgramWorkflowMember.email) \
            .filter(ProgramWorkflowMember.member_type == APPROVER) \
            .filter(ProgramWorkflowMember.is_deleted == False) \
            .filter(ProgramWorkflowMember.program_workflow_id == workflow_member.program_workflow_id) \
            .first()

        if approver_query is not None and approver_query[0] is not None:
            approver_name = approver_query.email
        else:
            approver_name_or_email_query = db_session.query(Users.name_first, Users.name_last, Users.email) \
                .filter(Users.id == ProgramWorkflowMember.user_id) \
                .filter(ProgramWorkflowMember.member_type == APPROVER) \
                .filter(ProgramWorkflowMember.is_deleted == False) \
                .filter(ProgramWorkflowMember.program_workflow_id == workflow_member.program_workflow_id) \
                .first()

            if approver_name_or_email_query is not None:
                approver_name_first, approver_name_last, approver_email = approver_name_or_email_query.name_first, \
                                                                 approver_name_or_email_query.name_last, \
                                                                 approver_name_or_email_query.email
                approver_name = '{} {}'.format(approver_name_first, approver_name_last) if approver_name_first \
                                                                                           and approver_name_last else approver_email

        variables["approver_name_full_or_email"] = approver_name

    if message_type == 'infl_review_reviewer_submits_infls':
        variables["reviewer_name_full_or_email"] = ""
        # if reviewer is guest, then just set the email variable as the email
        if reviewer.email is not None:
            variables["reviewer_name_full_or_email"] = reviewer.email
        else:
            reviewer_name_or_email_query = db_session.query(Users.name_first, Users.name_last, Users.email) \
                .filter(Users.id == reviewer.user_id) \
                .first()

            if reviewer_name_or_email_query is not None:
                name_first, name_last, email = reviewer_name_or_email_query.name_first, reviewer_name_or_email_query.name_last, \
                                               reviewer_name_or_email_query.email
                variables["reviewer_name_full_or_email"] = '{} {}'.format(name_first, name_last) if name_first and name_last else email
    try:
        send_notification(message_type, workflow_member.user_id, variables, message_key, to_email=to_email)
    except Exception ,e:
        raise CrudResponsePlain("Error while sending '{}' notification for Workflow Member {}: {}"
                                .format(message_type, workflow_member.id, e), status_code=400)
