package cn.edu.sustech.cs307.exception;

/**
 *  in remove method, if there is no Entity about specific, throw EntityNotFoundException.
 *  in get method, if there is no Entity about specific id, throw EntityNotFoundException.
 */
public class EntityNotFoundException extends RuntimeException
{

}
