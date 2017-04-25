package TweetsExtraction;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import twitter4j.FilterQuery;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.User;
import twitter4j.conf.ConfigurationBuilder;



/**
 *
 * @author RAKEZ
 */
public class Infosyatem {
   
    static Statement stmt;
    
    static Connection conn;
    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        // TODO code application logic here
          try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (Exception ex) {
            System.out.println("Failed "+ ex);
        }
          
    if(conn==null){
        
    
    try {
    conn =
       DriverManager.getConnection("jdbc:mysql://localhost:3306/tweetapp","root","");

    stmt = conn.createStatement();
    System.out.println("Successfult connected");
  
    } catch (SQLException ex) {
    // handle any errors
    System.out.println("SQLException: " + ex.getMessage());
    System.out.println("SQLState: " + ex.getSQLState());
    System.out.println("VendorError: " + ex.getErrorCode());
    
    }
    }
        
        
        
        
        
        final List<String> tweet = new ArrayList();
        final List<String> tweetuser =new ArrayList();
        final List<Long> tweetid =new ArrayList();
        final List<String> tweetloc =new ArrayList();
        
        
        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setDebugEnabled(true);
        cb.setOAuthConsumerKey("vbabv3mgkoBrshODzfNDzj0J");
        cb.setOAuthConsumerSecret("j2PZlQOtLBP4Xtilj4ZesvGxs0FPio5x0VOv6M3mCecejCv7y");
        cb.setOAuthAccessToken("4542181215-N3FQH2a1obO307c2viH9vcDIrBBoxptbR47vP8");
        cb.setOAuthAccessTokenSecret("TDnqbVrL4uf0DYS61oVeKa3JfvYrVBCSOSu7n0FSa3g8");
        
        
         final TwitterStream twitterStream = new TwitterStreamFactory(cb.build()).getInstance();
         StatusListener listener = new StatusListener(){
             

            @Override
            public void onStatus(Status status) {
               
                User user = status.getUser();
                
                // gets Username
                String username = status.getUser().getScreenName();
                tweetuser.add(username);
                System.out.println(username);
                String profileLocation = user.getLocation();
                tweetloc.add(profileLocation);
                System.out.println(profileLocation);
                long tweetId = status.getId(); 
                tweetid.add(tweetId);
                System.out.println(tweetId);
                String content = status.getText();
                System.out.println(content +"\n");
                tweet.add(content);
                if(tweet.size()>100){
                    
                    Infosyatem inf = new Infosyatem();
                    inf.insertdata(tweetuser,tweetloc,tweetid,tweet);
                    inf.displaydata(tweet);
                    twitterStream.shutdown();
                    
                }
                
                

            
                //To change body of generated methods, choose Tools | Templates.
            }

            @Override
            public void onDeletionNotice(StatusDeletionNotice sdn) {
                throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
            }

            @Override
            public void onTrackLimitationNotice(int i) {
                throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
            }

            @Override
            public void onScrubGeo(long l, long l1) {
                throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
            }

            @Override
            public void onStallWarning(StallWarning sw) {
                throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
            }

            @Override
            public void onException(Exception excptn) {
                System.out.println("Error is:"  + excptn); //To change body of generated methods, choose Tools | Templates.
            }

            
             
         
                 
         
       
        
        
         };
        FilterQuery fq = new FilterQuery();
    
        String keywords[] = {"terror on London","parris attack"};

        fq.track(keywords);

        twitterStream.addListener(listener);
        twitterStream.filter(fq); 
        
      
         
        }
    
    private void displaydata(List<String> tweet) {
        for(int i=0;i<tweet.size();i++){
            System.out.println("tewws"+tweet.get(i));
        }
    }

    private void insertdata(List<String> tweetuser, List<String> tweetloc, List<Long> tweetid, List<String> tweet) {
        
      for(int i=0;i<tweet.size();i++){  
        try{
            PreparedStatement st = conn.prepareStatement("insert into tweets(tweetloc,tweetuser,tweet,tweetid) values(?,?,?,?)");
            System.out.println("checking var");
            
            
            st.setString(1, tweetloc.get(i));
            st.setString(2, tweetuser.get(i));
            st.setString(4, tweet.get(i));
            st.setLong(3,tweetid.get(i));
            st.execute();
        
        }catch(Exception e){
            System.out.println(e);
        }
      } 
     
    
    }
    
    
    
    
}
