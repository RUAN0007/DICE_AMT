import java.util.*;
import com.amazonaws.mturk.addon.HITDataCSVReader;
import com.amazonaws.mturk.addon.HITDataCSVWriter;
import com.amazonaws.mturk.addon.HITDataInput;
import com.amazonaws.mturk.addon.HITTypeResults;
import com.amazonaws.mturk.dataschema.QuestionFormAnswers;
import com.amazonaws.mturk.dataschema.QuestionFormAnswersType;
import com.amazonaws.mturk.requester.Assignment;
import com.amazonaws.mturk.requester.AssignmentStatus;
import com.amazonaws.mturk.service.axis.RequesterService;
import com.amazonaws.mturk.util.PropertiesClientConfig;
import com.amazonaws.mturk.service.exception.ServiceException;
import com.amazonaws.mturk.requester.HIT;

public class TurkManager {

  private SQLiteJDBC sqlite_ = null;
  private RequesterService service_;

  // Define the properties of the HIT to be created.
  private int numAssignments_ = 1;
  private double reward_ = 0.01;
  private String title_ = "Question From DICE";
  private String description_ = " Question description";

  public TurkManager(SQLiteJDBC sqlite){
    this.sqlite_ = sqlite;
    this.service_ = new RequesterService(new PropertiesClientConfig("mturk.properties"));
  }

  /*
  Get questions from CSTask_Turk with INIT status

  Returns: a map of unposted questions
    key: questionID
    value: Turk QuestionForm XML in string format
  */
  private Map<String, String>  getUnpostedQuestions() {
    String sql = "SELECT qid, sub FROM CSTask_Turk WHERE status = 'INIT'";

    Map<String, List<String>> queryResult = this.sqlite_.diceSelect(sql);

    Map<String, String> unpostedQuestions = new HashMap<>();

    List<String> qids= queryResult.get("qid");
    List<String> xmls = queryResult.get("sub");

    for(int i = 0;i < qids.size();i++) {
      String qid = qids.get(i);
      String xml  = xmls.get(i);

      unpostedQuestions.put(qid, xml);
    }

    // System.out.println("Unposted Question: " + unpostedQuestions.toString());
    return unpostedQuestions;
  }

  /*
  For questions that have been successfully posted to Turk
    Update Status field from INIT to POSTED
    Record down their hitId and hitURL 
      Update Info field to <hitIds>_<hitURL> 
  */
  private boolean updatePostedQuestionStatus(List<String> qids, List<String> hitIds, List<String> hitURLs){

    if (qids.size() == 0) return false;

    String qidList = "(";
    qidList += String.join(", ", qids);
    qidList += ") ";

    String caseClause = " CASE qid ";

    for(int i = 0;i < qids.size(); i++) {
      String qid = qids.get(i);
      String hitId = hitIds.get(i);
      String hitURL = hitURLs.get(i);

      String infoField = hitId + "_" + hitURL; 

      caseClause += "WHEN " + qid + " THEN ";
      caseClause += "'" +infoField + "' ";
    }

    caseClause += "END ";

    String sql = "UPDATE CSTask_Turk SET ";
    sql += "STATUS = 'POSTED', ";
    sql += "INFO = " + caseClause + ", ";
    sql += "POST_TIME = CURRENT_TIMESTAMP ";
    sql += " WHERE qid in " + qidList + ";";

    return this.sqlite_.diceExec(sql);
  }

  /*
  * Retrieve Unposted questions from DICE DB. (Question with status INIT)
  * Create HIT for each questios.
  * Update the successfully posted questions for their HitIDs and HitURL
  */
  public boolean postQuestions(){
    Map<String, String> newQuestions = this.getUnpostedQuestions();

    List<String> qids = new ArrayList<>(); 
    List<String> hitIds = new ArrayList<>(); 
    List<String> hitURLs = new ArrayList<>(); 

    for(Map.Entry<String, String> question: newQuestions.entrySet()) {
      String qid =  question.getKey();
      String questionXML = question.getValue(); 

      try{

        HIT hit = service_.createHIT(
                title_,
                description_,
                reward_,
                questionXML,
                numAssignments_);

        String hitID = hit.getHITId();
        String hitURL = service_.getWebsiteURL() 
          + "/mturk/preview?groupId=" + hit.getHITTypeId();

        System.out.println("Access Question " + qid + " at " + hitURL);
        qids.add(qid);
        hitIds.add(hitID);
        hitURLs.add(hitURL);
      }catch (ServiceException e) {
        System.err.println("Fail to Upload Question " + qid + " with XML: ");
        System.err.println(questionXML);
        System.err.println();
        System.err.println(e.getLocalizedMessage());
      }//End of try
    }//End of for

    return this.updatePostedQuestionStatus(qids, hitIds, hitURLs);
  }
  /*
  Retrieve the answer of posted questions on Turk and approve the answer

  Return: 
    a map with questionID as key and answer as value
  */
  private Map<String, String>  getTurkAnswers() {

    String sql = " SELECT qid, info FROM CSTask_Turk WHERE Status = 'POSTED';";
    Map<String, List<String>> queryResult = this.sqlite_.diceSelect(sql);

    List<String> qids = queryResult.get("qid");
    List<String> infos = queryResult.get("info");

    Map<String, String> answers = new HashMap<>();
    for(int i = 0;i < qids.size();i++) {
      String qid = qids.get(i);
      String info = infos.get(i);

      String hitID = info.split("_")[0];

      //Only consider the first assignment for each question
      Assignment[] assignments = service_.getAllAssignmentsForHIT(hitID);

      if (assignments.length == 0) continue; //This question has not been answered. 
      Assignment assignment = assignments[0];

      //Ignore questions that have not been submitted. 
      if (assignment.getAssignmentStatus() != AssignmentStatus.Submitted) continue;

      String answerXML = assignment.getAnswer();

      QuestionFormAnswers qfa = RequesterService.parseAnswers(answerXML);
      List<QuestionFormAnswersType.AnswerType> turkAnswers = (List<QuestionFormAnswersType.AnswerType>) qfa.getAnswer();

      QuestionFormAnswersType.AnswerType turkAnswer = turkAnswers.get(0);

      String assignmentId = assignment.getAssignmentId();
      String answerValue = RequesterService.getAnswerValue(assignmentId, turkAnswer);

      answers.put(qid, answerValue);
      service_.approveAssignment(assignmentId, "Well Done!");
      System.out.println("Question " + qid + " Answer: " + answerValue);

     }//End of for

     return answers;
  }

/*
  Write the retrieved answers to DICE relaton CSTask_Turk
*/
  private boolean updateAnswers(Map<String, String> answers) {

    if (answers.size() == 0) return false;

    List<String> qids = new ArrayList<>();
    String caseClause = " CASE qid ";

    for(Map.Entry<String, String> answer : answers.entrySet()) {
      String qid = answer.getKey();
      String answerText = answer.getValue();

      qids.add(qid);

      caseClause += "WHEN " + qid + " THEN ";
      caseClause += "'" +answerText + "' ";
    }
    caseClause += "END ";

    String qidList = "(";
    qidList += String.join(", ", qids);
    qidList += ") ";

    String sql = "UPDATE CSTask_Turk SET ";
    sql += "STATUS = 'FINISH', ";
    sql += "RESULT = " + caseClause + ", ";
    sql += "FINISH_TIME = CURRENT_TIMESTAMP, ";
    sql += "COST =  " + reward_ + " ";
    sql += "WHERE qid in " + qidList + ";";

    return this.sqlite_.diceExec(sql);
  }

  /*
  * Retrieve submitted answers for posted questions in Turk platform
  * Update the retrieved answers to DICE relation
  * Return:
      a set of question ID whose answers have been retrieved and update
  */
  public Set<String> retrieveAnswers() {
    Map<String,String> answers = this.getTurkAnswers();
    if (this.updateAnswers(answers)) {
      return answers.keySet();
    }else{
      return new HashSet();//Return an empty set if updating questions failed.
    }
  }  

   // public static void main (String args[]) {
   //   SQLiteJDBC sqlite = new SQLiteJDBC("/home/ruanpingcheng/Desktop/sqliteDB/dice.db");
   //   TurkManager turk = new TurkManager(sqlite);

   //   turk.postQuestions();
   //   Set<String> anss = turk.retrieveAnswers();
   //   System.out.println("Answers: " + anss);
   //   List<String> qids = Arrays.asList("1575","1245");
   //   List<String> hitIDs = Arrays.asList("hitID1","hitId2");
   //   List<String> hitURLs = Arrays.asList("hitURL1","hitURL2");

   //   turk.updatePostedQuestionStatus(qids, hitIDs, hitURLs);
   // }
}
