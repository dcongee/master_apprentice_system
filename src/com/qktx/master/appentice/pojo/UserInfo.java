package com.qktx.master.appentice.pojo;

public class UserInfo {
	Long parentUserID;
	Long userID;
	Integer level;

	public UserInfo(Long parentUserID, Long userID, Integer level) {
		this.parentUserID = parentUserID;
		this.userID = userID;
		this.level = level;
	}

	public UserInfo() {
		// TODO Auto-generated constructor stub
	}

	public Long getParentUserID() {
		return parentUserID;
	}

	public void setParentUserID(Long parentUserID) {
		this.parentUserID = parentUserID;
	}

	public Long getUserID() {
		return userID;
	}

	public void setUserID(Long userID) {
		this.userID = userID;
	}

	public Integer getLevel() {
		return level;
	}

	public void setLevel(Integer level) {
		this.level = level;
	}

}
